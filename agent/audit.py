"""Registro de auditoria del agente.

Por cada pregunta queda una linea JSON en agent/logs/audit.jsonl con:
  - la pregunta y la respuesta completa que se mostro;
  - el modelo, el effort y la huella del dataset sobre el que se respondio;
  - cada herramienta que llamo el modelo, con sus argumentos;
  - cada consulta SQL que ejecuto esa herramienta, tal como corrio (plantilla
    + parametros) y renderizada para copiar y pegar, con la cantidad de filas
    y un hash SHA-256 del resultado;
  - el hash del resultado de cada herramienta y los tokens consumidos.

La respuesta en lenguaje natural puede variar entre corridas -- es un modelo
de lenguaje --, pero lo que esta debajo no: la misma pregunta sobre el mismo
dataset tiene que producir el mismo SQL y los mismos hashes. `replay` lo
demuestra: vuelve a ejecutar el SQL registrado y compara.

Uso
---
    python -m agent.audit list            # ultimas preguntas registradas
    python -m agent.audit show <id>       # pregunta, SQL ejecutado y respuesta
    python -m agent.audit replay <id>     # re-ejecuta el SQL y compara hashes
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
import textwrap
import threading
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_PATH = LOG_DIR / "audit.jsonl"

_lock = threading.Lock()
_current: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# serializacion y huellas
# ---------------------------------------------------------------------------
def _default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dt.date | dt.datetime):
        return value.isoformat()
    raise TypeError(f"Tipo no serializable: {type(value).__name__}")


def canonical(payload: Any) -> str:
    """JSON estable: mismas filas en el mismo orden producen el mismo texto."""
    return json.dumps(payload, ensure_ascii=False, default=_default, separators=(",", ":"))


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def clean_sql(sql: str) -> str:
    return textwrap.dedent(sql).strip()


def render_sql(sql: str, params: list[Any]) -> str:
    """Sustituye los `?` por literales, para poder copiar la consulta y correrla.

    Solo se usa para mostrar y para replay. La ejecucion real siempre va con
    parametros, nunca con texto armado a mano.
    """
    rendered: list[str] = []
    values = iter(params)
    for piece in sql.split("?")[:-1]:
        rendered.append(piece)
        value = next(values)
        if value is None:
            rendered.append("null")
        elif isinstance(value, bool):
            rendered.append("true" if value else "false")
        elif isinstance(value, int | float | Decimal):
            rendered.append(str(value))
        else:
            rendered.append("'" + str(value).replace("'", "''") + "'")
    rendered.append(sql.split("?")[-1])
    return "".join(rendered)


# ---------------------------------------------------------------------------
# captura (la llaman chat.py y tools.py)
# ---------------------------------------------------------------------------
def begin(question: str, **context: Any) -> str:
    """Abre el registro de una pregunta y devuelve su id."""
    global _current
    question_id = uuid.uuid4().hex[:10]
    with _lock:
        _current = {
            "id": question_id,
            "timestamp": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            "question": question,
            **context,
            "tool_calls": [],
        }
    return question_id


def record_call(
    tool: str,
    arguments: dict[str, Any],
    queries: list[dict[str, Any]],
    duration_ms: float,
    result: str | None = None,
    error: str | None = None,
) -> None:
    """Agrega una llamada a herramienta a la pregunta en curso (si hay una)."""
    with _lock:
        if _current is None:
            return
        _current["tool_calls"].append(
            {
                "tool": tool,
                "arguments": arguments,
                "duration_ms": round(duration_ms, 1),
                "queries": queries,
                "result_sha256": sha256(result) if result is not None else None,
                "result_chars": len(result) if result is not None else None,
                "error": error,
            }
        )


def finish(answer: str, **outcome: Any) -> dict[str, Any] | None:
    """Cierra la pregunta en curso y la escribe en el log."""
    global _current
    with _lock:
        record, _current = _current, None
    if record is None:
        return None
    record["answer"] = answer
    record.update(outcome)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, default=_default) + "\n")
    return record


# ---------------------------------------------------------------------------
# lectura
# ---------------------------------------------------------------------------
def read_log() -> list[dict[str, Any]]:
    if not LOG_PATH.exists():
        return []
    with LOG_PATH.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def find(question_id: str) -> dict[str, Any]:
    matches = [r for r in read_log() if r["id"].startswith(question_id)]
    if not matches:
        raise SystemExit(f"No hay ninguna pregunta registrada con id {question_id!r}.")
    if len(matches) > 1:
        raise SystemExit(f"El id {question_id!r} es ambiguo: coincide con {len(matches)} preguntas.")
    return matches[0]


def cmd_list(limit: int) -> int:
    records = read_log()[-limit:]
    if not records:
        print(f"Todavia no hay preguntas registradas en {LOG_PATH}.")
        return 0
    for r in records:
        tools_used = ", ".join(c["tool"] for c in r["tool_calls"]) or "-"
        print(f"{r['id']}  {r['timestamp']}  corte {r.get('ledger_cutoff_date')}  [{tools_used}]")
        print(f"            {r['question']}")
    return 0


def cmd_show(question_id: str) -> int:
    r = find(question_id)
    print(f"Pregunta {r['id']}  ({r['timestamp']})")
    print(f"Modelo {r.get('model')} | effort {r.get('effort')} | corte {r.get('ledger_cutoff_date')}")
    print(f"Huella del dataset: {r.get('dataset_sha256')}")
    print(f"\n> {r['question']}\n")
    for n, call in enumerate(r["tool_calls"], 1):
        args = ", ".join(f"{k}={v!r}" for k, v in call["arguments"].items())
        print(f"[{n}] {call['tool']}({args})  {call['duration_ms']} ms")
        if call["error"]:
            print(f"    ERROR devuelto al modelo: {call['error']}")
        else:
            print(f"    resultado sha256 {call['result_sha256'][:16]}... ({call['result_chars']} caracteres)")
        for q in call["queries"]:
            print(f"    -- {q['rows']} fila(s), sha256 {q['result_sha256'][:16]}...")
            print(textwrap.indent(q["sql_rendered"], "    ") + ";")
        print()
    print("Respuesta:")
    print(textwrap.indent(r["answer"] or "(sin texto)", "  "))
    if r.get("usage"):
        print(f"\nTokens: {r['usage']}")
    return 0


def cmd_replay(question_id: str) -> int:
    """Re-ejecuta el SQL y las herramientas de una pregunta y compara contra el log."""
    from agent import ledger, tools

    r = find(question_id)
    con, state = ledger.load()
    tools.bind(con)
    fingerprint = ledger.dataset_fingerprint(state)
    same_data = fingerprint == r.get("dataset_sha256")
    print(f"Replay de {r['id']}: {r['question']}")
    print(f"Dataset {'IDENTICO' if same_data else 'DISTINTO'} al de la respuesta original "
          f"({fingerprint[:16]}... vs {str(r.get('dataset_sha256'))[:16]}...)\n")

    differences = 0
    by_name = {fn.__name__: fn for fn in tools.FUNCTIONS}
    for n, call in enumerate(r["tool_calls"], 1):
        print(f"[{n}] {call['tool']}({call['arguments']})")
        for q in call["queries"]:
            rows = tools.run_sql(q["sql"], q["params"])
            digest = sha256(canonical(rows))
            ok = digest == q["result_sha256"] and len(rows) == q["rows"]
            differences += not ok
            print(f"    SQL      {'IGUAL    ' if ok else 'DISTINTO '} {len(rows)} fila(s)  {digest[:16]}...")
        if call["error"] is None:
            output = by_name[call["tool"]](**call["arguments"])
            ok = sha256(output) == call["result_sha256"]
            differences += not ok
            print(f"    resultado {'IGUAL   ' if ok else 'DISTINTO'} {sha256(output)[:16]}...")

    print()
    if differences:
        print(f"{differences} diferencia(s).", "Esperable: cambio el dataset." if not same_data else "")
        return 1
    print("Todo identico: mismo SQL, mismas filas, mismo resultado.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Registro de auditoria del agente.")
    sub = parser.add_subparsers(dest="command", required=True)
    list_parser = sub.add_parser("list", help="ultimas preguntas registradas")
    list_parser.add_argument("-n", type=int, default=20, help="cuantas mostrar")
    sub.add_parser("show", help="detalle de una pregunta").add_argument("id")
    sub.add_parser("replay", help="re-ejecuta el SQL de una pregunta").add_argument("id")
    args = parser.parse_args(argv)

    if args.command == "list":
        return cmd_list(args.n)
    if args.command == "show":
        return cmd_show(args.id)
    return cmd_replay(args.id)


if __name__ == "__main__":
    sys.exit(main())
