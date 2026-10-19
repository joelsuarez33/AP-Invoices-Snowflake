"""Agente de terminal: preguntas en lenguaje natural sobre el ledger de cuentas a pagar.

Claude responde llamando a las herramientas tipadas de agent/tools.py -- nunca
escribe SQL -- sobre la base local de agent/ledger.py. No necesita Snowflake:
el ledger se arma con el estado que publica el pipeline en la rama artifacts.

Uso
---
    python -m agent.chat                                # sesion interactiva
    python -m agent.chat --pregunta "cuanto esta vencido?"  # una pregunta y sale

En la sesion: /nuevo borra el historial, /uso muestra los tokens, /salir termina.

Variables de entorno (se leen de .env)
--------------------------------------
    ANTHROPIC_API_KEY   obligatoria
    AGENT_MODEL         modelo, por defecto claude-haiku-5-5
    AGENT_EFFORT        low | medium | high, por defecto medium
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anthropic
from dotenv import dotenv_values, load_dotenv

from agent import audit, ledger, tools

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

MODEL = os.environ.get("AGENT_MODEL", "").strip() or "claude-haiku-5-5"
EFFORT = os.environ.get("AGENT_EFFORT", "").strip() or "medium"
MAX_TOKENS = 16000

SYSTEM_TEMPLATE = """\
Sos un analista de cuentas a pagar. Respondes preguntas sobre el ledger de \
acreedores de una empresa argentina, cuyo libro cierra en pesos.

Los datos simulan el extracto de partidas de acreedores de SAP (transaccion \
FBL1N) y son sinteticos. El ledger esta al {cutoff}: esa es la fecha de corte, \
y toda antiguedad se mide contra ella, no contra la fecha de hoy.

Como trabajar:
- Toda cifra sale de una herramienta, incluidos porcentajes, proporciones y \
comparaciones ("mas de la mitad", "el doble"). No hagas cuentas propias sobre \
los resultados: si la herramienta no trae el dato, no lo afirmes. Las \
herramientas ya traen los porcentajes de participacion (pct_*): usa esos. Si \
ninguna herramienta responde la pregunta, decilo en vez de estimar.
- Tampoco ordenes, filtres ni cuentes filas por tu cuenta. Si necesitas otro \
orden u otro filtro, usa los parametros de la herramienta (por ejemplo \
orden=importe_moneda_doc para las mas grandes en dolares). Si la herramienta \
no lo permite, decilo.
- El generador del dataset limita cada documento a $ 25.000.000. Muchos \
documentos tienen exactamente ese importe en pesos y distinto importe en \
moneda extranjera: es correcto, porque cada uno se convirtio a la cotizacion \
de su fecha. No lo marques como inconsistencia.
- Los codigos de acreedor (LIFNR) son numeros de 4 digitos. "Proveedor 017" es \
un nombre, no un codigo: buscalo con buscar_acreedor.
- Si el usuario nombra un proveedor, busca su codigo con buscar_acreedor antes \
de pedir su detalle.
- Los importes estan en pesos. Las facturas tienen signo negativo porque son \
deuda con el proveedor: al responder, da el monto en valor absoluto y deja \
claro que es deuda. Usa formato argentino ($ 1.234.567).
- Una revaluacion cambiaria negativa es perdida: la deuda en moneda extranjera \
vale mas pesos que cuando se contabilizo.
- Respuestas cortas y directas, en espanol rioplatense. Una tabla solo cuando \
compares varias filas.
"""


@dataclass
class Usage:
    requests: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read: int = 0
    cache_write: int = 0

    def add(self, usage: Any) -> None:
        self.requests += 1
        self.input_tokens += usage.input_tokens or 0
        self.output_tokens += usage.output_tokens or 0
        self.cache_read += getattr(usage, "cache_read_input_tokens", 0) or 0
        self.cache_write += getattr(usage, "cache_creation_input_tokens", 0) or 0

    def snapshot(self) -> dict[str, int]:
        return dict(vars(self))

    def since(self, before: dict[str, int]) -> dict[str, int]:
        """Tokens consumidos desde un snapshot: el costo de una sola pregunta."""
        return {key: value - before[key] for key, value in vars(self).items()}

    def __str__(self) -> str:
        return (
            f"{self.requests} llamada(s) | entrada {self.input_tokens:,} | salida {self.output_tokens:,} "
            f"| cache leida {self.cache_read:,} | cache escrita {self.cache_write:,}"
        )


def _describe_call(block: Any) -> str:
    args = ", ".join(f"{k}={v!r}" for k, v in (block.input or {}).items())
    return f"{block.name}({args})"


class Session:
    """Una conversacion. El historial es append-only: nunca se edita un turno ya enviado."""

    def __init__(
        self, client: anthropic.Anthropic, system: str, cutoff: str, dataset_sha256: str
    ) -> None:
        self.client = client
        self.system = system
        self.cutoff = cutoff
        self.history: list[dict[str, Any]] = []
        self.usage = Usage()
        self.session_id = uuid.uuid4().hex[:8]
        self.dataset_sha256 = dataset_sha256

    def ask(self, question: str) -> None:
        checkpoint = len(self.history)
        before = self.usage.snapshot()
        question_id = audit.begin(
            question,
            session_id=self.session_id,
            turn=sum(1 for m in self.history if m["role"] == "user" and isinstance(m["content"], str)) + 1,
            model=MODEL,
            effort=EFFORT,
            ledger_cutoff_date=self.cutoff,
            dataset_sha256=self.dataset_sha256,
        )
        self.history.append({"role": "user", "content": question})
        try:
            answer, stop_reason = self._run()
        except BaseException as exc:
            # Un turno a medio terminar (un tool_use sin su tool_result) haria
            # fallar la proxima pregunta. Se descarta entero: es la cola del
            # historial, nunca un turno anterior ya respondido.
            del self.history[checkpoint:]
            audit.finish("", error=f"{type(exc).__name__}: {exc}", usage=self.usage.since(before))
            raise
        audit.finish(answer, stop_reason=stop_reason, usage=self.usage.since(before))
        print(f"[auditoria {question_id}: python -m agent.audit show {question_id}]")

    def _run(self) -> tuple[str, str | None]:
        runner = self.client.beta.messages.tool_runner(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=self.system,
            tools=tools.TOOLS,
            messages=list(self.history),
            output_config={"effort": EFFORT},
            # Cachea el prefijo estable (herramientas + system + historial).
            cache_control={"type": "ephemeral"},
            stream=True,
        )
        printed_text = False
        answer_parts: list[str] = []
        stop_reason: str | None = None
        for stream in runner:
            for event in stream:
                if event.type == "text":
                    print(event.text, end="", flush=True)
                    printed_text = True
            message = stream.get_final_message()
            self.usage.add(message.usage)
            stop_reason = message.stop_reason
            answer_parts.extend(b.text for b in message.content if b.type == "text")

            # El turno del asistente se guarda tal cual, con sus bloques de
            # thinking: el modelo los necesita intactos en el proximo pedido.
            self.history.append({"role": "assistant", "content": message.content})

            if message.stop_reason == "refusal":
                print("\n[El modelo declino responder esta pregunta.]")
                return "".join(answer_parts), stop_reason
            calls = [b for b in message.content if b.type == "tool_use"]
            if message.stop_reason == "max_tokens" and calls:
                raise RuntimeError("La respuesta se corto antes de completar una llamada a herramienta.")
            for call in calls:
                prefix = "\n" if printed_text else ""
                print(f"{prefix}  -> {_describe_call(call)}", flush=True)
                printed_text = False

            tool_results = runner.generate_tool_call_response()
            if tool_results is not None:
                self.history.append(tool_results)
        print()
        return "".join(answer_parts), stop_reason


def build_session() -> Session:
    con, state = ledger.load()
    tools.bind(con)
    checks = ledger.reconcile(con, state)
    failed = [c["control"] for c in checks if c["ok"] is False]
    if failed:
        print(f"Aviso: la capa local no cuadra con la mart en: {', '.join(failed)}.")
    system = SYSTEM_TEMPLATE.format(cutoff=state["last_run_date"])
    return Session(
        anthropic.Anthropic(), system, state["last_run_date"], ledger.dataset_fingerprint(state)
    )


def _ask(session: Session, question: str) -> bool:
    """Devuelve False si el error impide seguir (por ejemplo, sin credenciales)."""
    try:
        session.ask(question)
    except anthropic.AuthenticationError as exc:
        # El mensaje de la API dice la causa real (key revocada, vencida, mal
        # copiada); un texto generico manda a buscar el problema equivocado.
        print(f"\nLa API rechazo la key: {exc.message}\nRevisar ANTHROPIC_API_KEY en el .env.")
        return False
    except anthropic.BadRequestError as exc:
        if "workspace" in str(exc.message).lower():
            # Una key con alcance de organizacion necesita el header
            # anthropic-workspace-id. Para el agente alcanza una key de un
            # espacio de trabajo, que ademas no tiene acceso a la Admin API.
            print(
                "\nLa key tiene alcance de organizacion y no indica un espacio de trabajo. "
                "Crear una key con alcance de espacio de trabajo y reemplazarla en el .env."
            )
            return False
        print(f"\nError de la API (400): {exc.message}")
    except anthropic.NotFoundError:
        print(f"\nEl modelo {MODEL!r} no existe o la key no tiene acceso. Revisar AGENT_MODEL.")
        return False
    except anthropic.RateLimitError:
        print("\nLimite de uso de la API. Esperar un momento y repetir la pregunta.")
    except anthropic.APIStatusError as exc:
        print(f"\nError de la API ({exc.status_code}): {exc.message}")
    except anthropic.APIConnectionError:
        print("\nSin conexion con la API. Revisar la red y repetir la pregunta.")
    except RuntimeError as exc:
        print(f"\n{exc}")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Agente de consulta sobre el ledger de cuentas a pagar.")
    parser.add_argument("--pregunta", help="hace una sola pregunta y sale")
    args = parser.parse_args(argv)

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print(f"Falta ANTHROPIC_API_KEY. Agregarla a {REPO_ROOT / '.env'}.", file=sys.stderr)
        return 1
    # load_dotenv no pisa una variable que ya existe en la sesion. Si quedo
    # una key vieja cargada (por ejemplo con scripts/load_env.ps1 antes de
    # cambiarla en el .env), se usaria esa sin ningun aviso.
    in_file = (dotenv_values(REPO_ROOT / ".env").get("ANTHROPIC_API_KEY") or "").strip()
    if in_file and in_file != os.environ["ANTHROPIC_API_KEY"].strip():
        print(
            "Aviso: ANTHROPIC_API_KEY de la sesion no coincide con la del .env y se usa la de la "
            "sesion. Para usar la del .env: Remove-Item Env:ANTHROPIC_API_KEY",
            file=sys.stderr,
        )

    try:
        session = build_session()
    except ledger.LedgerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.pregunta:
        return 0 if _ask(session, args.pregunta) else 1

    print(f"Ledger de cuentas a pagar | modelo {MODEL} | effort {EFFORT}")
    print("Preguntas en lenguaje natural. /nuevo borra el historial, /uso muestra tokens, /salir termina.\n")
    while True:
        try:
            question = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not question:
            continue
        if question in {"/salir", "/exit", "/quit"}:
            break
        if question == "/nuevo":
            session.history.clear()
            print("Historial borrado.\n")
            continue
        if question == "/uso":
            print(f"{session.usage}\n")
            continue
        try:
            if not _ask(session, question):
                return 1
        except KeyboardInterrupt:
            print("\n[Interrumpido.]")
        print()

    print(session.usage)
    return 0


if __name__ == "__main__":
    sys.exit(main())
