"""Herramientas tipadas del agente.

Cada herramienta es una consulta fija y parametrizada sobre la tabla `items`
de agent/ledger.py. El modelo no escribe SQL: elige una herramienta y completa
sus parametros, que el SDK valida contra la firma tipada antes de ejecutar.
Eso acota lo que se puede preguntar, pero hace imposible que una respuesta
salga de una consulta mal armada -- o que una consulta toque algo que no debe.

Las herramientas devuelven JSON. Los importes van en pesos (DMBTR) con el
signo del ledger: negativo es deuda con el proveedor. Como formatearlos para
una persona lo decide el system prompt, no la herramienta.
"""

from __future__ import annotations

import contextvars
import datetime as dt
import functools
import json
import re
import time
from collections.abc import Callable
from decimal import Decimal
from typing import Any, Literal

import duckdb
from anthropic import beta_tool
from anthropic.lib.tools import ToolError

from agent import audit
from agent.ledger import AGING_BUCKETS, SUMMARY_SQL

# Un parametro invalido no es una excepcion del programa: es informacion para
# el modelo. ToolError llega como tool_result con is_error y el texto tal cual,
# sin el repr() de la excepcion, y el modelo corrige la llamada.

MAX_ROWS = 100

_connection: duckdb.DuckDBPyConnection | None = None


def bind(con: duckdb.DuckDBPyConnection) -> None:
    """Conecta las herramientas a la base que armo agent.ledger.load()."""
    global _connection
    _connection = con


def _con() -> duckdb.DuckDBPyConnection:
    if _connection is None:
        raise RuntimeError("Herramientas sin base: llamar a tools.bind(con) antes de usarlas.")
    return _connection


def run_sql(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    """Ejecuta una consulta parametrizada y devuelve las filas como dicts.

    Publica para que audit.replay pueda re-ejecutar lo que quedo registrado.
    """
    cur = _con().execute(sql, params or [])
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


# Consultas de la herramienta en curso. Es un ContextVar y no una lista global
# para que una herramienta que llama a otra (detalle_acreedor usa
# antiguedad_deuda) junte todas sus consultas en una sola llamada auditada.
_queries: contextvars.ContextVar[list[dict[str, Any]] | None] = contextvars.ContextVar(
    "queries", default=None
)


def _rows(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    params = params or []
    rows = run_sql(sql, params)
    captured = _queries.get()
    if captured is not None:
        sql = audit.clean_sql(sql)
        captured.append(
            {
                "sql": sql,
                "params": params,
                "sql_rendered": audit.render_sql(sql, params),
                "rows": len(rows),
                "result_sha256": audit.sha256(audit.canonical(rows)),
            }
        )
    return rows


def _default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dt.date):
        return value.isoformat()
    raise TypeError(f"Tipo no serializable: {type(value).__name__}")


def _json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=_default)


def _limit(value: int) -> int:
    if not 1 <= value <= MAX_ROWS:
        raise ToolError(f"limite tiene que estar entre 1 y {MAX_ROWS}, llego {value}")
    return value


def _vendor_exists(lifnr: str) -> None:
    if not _rows("select 1 from vendors where lifnr = ?", [lifnr]):
        raise ToolError(
            f"No existe el acreedor {lifnr!r}. Usar buscar_acreedor para encontrar el codigo."
        )


# ---------------------------------------------------------------------------
# herramientas
# ---------------------------------------------------------------------------
def resumen_ledger() -> str:
    """Totales del ledger de cuentas a pagar a la fecha de corte.

    Devuelve partidas totales, abiertas y compensadas, saldo pendiente, saldo
    vencido, partidas vencidas, revaluacion cambiaria y cantidad de acreedores.
    Usar para preguntas generales sobre el estado de la deuda.
    """
    return _json(_rows(SUMMARY_SQL)[0])


def antiguedad_deuda(lifnr: str | None = None) -> str:
    """Antiguedad (aging) de las partidas abiertas, por tramo de dias de vencido.

    Los tramos son No vencida, Vence hoy, 1-30, 31-60, 61-90 y 90+. Los dias se
    cuentan contra la fecha de corte del ledger, no contra la fecha de hoy.

    Args:
        lifnr: Codigo de acreedor (LIFNR) para ver solo su deuda. Omitir para todo el ledger.
    """
    params: list[Any] = []
    where = "is_open"
    if lifnr is not None:
        _vendor_exists(lifnr)
        where += " and lifnr = ?"
        params.append(lifnr)
    return _json(
        _rows(
            f"""
            select
                aging_bucket                    as tramo,
                count(*)                        as partidas,
                round(sum(dmbtr), 2)            as saldo_ars,
                round(100.0 * sum(dmbtr) / nullif(sum(sum(dmbtr)) over (), 0), 1)
                                                as pct_del_saldo_abierto,
                round(avg(days_overdue), 1)     as dias_vencido_promedio
            from items
            where {where}
            group by aging_bucket, aging_bucket_sort
            order by aging_bucket_sort
            """,
            params,
        )
    )


def buscar_acreedor(texto: str) -> str:
    """Busca acreedores por nombre, codigo (LIFNR) o pais.

    Usar antes de cualquier herramienta que pida un lifnr cuando el usuario
    nombra al proveedor en vez de dar el codigo.

    Args:
        texto: Parte del nombre, del codigo o el codigo de pais (por ejemplo "017", "SA", "BR").
    """
    pattern = f"%{texto.strip().lower()}%"
    return _json(
        _rows(
            """
            select lifnr, vendor_name as nombre, country as pais,
                   payment_terms as condicion_pago, agreed_payment_days as dias_pactados
            from vendors
            where lower(vendor_name) like ? or lifnr like ? or lower(country) = ?
            order by vendor_name
            limit 20
            """,
            [pattern, pattern, texto.strip().lower()],
        )
    )


def ranking_acreedores(
    criterio: Literal["saldo_abierto", "saldo_vencido", "revaluacion_fx", "partidas_abiertas"],
    limite: int = 10,
) -> str:
    """Ranking de acreedores con deuda abierta, de mayor a menor segun un criterio.

    Incluye pct_del_total_del_ledger: la participacion de cada acreedor en el
    total del ledger para ese criterio.

    Args:
        criterio: saldo_abierto (deuda pendiente), saldo_vencido (deuda ya vencida), revaluacion_fx (perdida por diferencia de cambio) o partidas_abiertas (cantidad).
        limite: Cuantos acreedores devolver, entre 1 y 100.
    """
    metric = {
        "saldo_abierto": "sum(dmbtr)",
        "saldo_vencido": "sum(case when days_overdue > 0 then dmbtr else 0 end)",
        "revaluacion_fx": "sum(coalesce(fx_revaluation_local, 0))",
        "partidas_abiertas": "count(*)",
    }[criterio]
    # El porcentaje sale de SQL y no del modelo: la ventana corre sobre todos
    # los acreedores antes del LIMIT, asi que es la participacion en el total
    # del ledger. En la primera prueba, el modelo calculo de cabeza "mas de la
    # mitad" para un 48%.
    return _json(
        _rows(
            f"""
            select
                lifnr,
                any_value(vendor_name)                                          as nombre,
                any_value(country)                                              as pais,
                count(*)                                                        as partidas_abiertas,
                round(sum(dmbtr), 2)                                            as saldo_abierto_ars,
                round(sum(case when days_overdue > 0 then dmbtr else 0 end), 2) as saldo_vencido_ars,
                round(sum(coalesce(fx_revaluation_local, 0)), 2)                as revaluacion_fx_ars,
                round(avg(days_overdue), 1)                                     as dias_vencido_promedio,
                round(100.0 * {metric} / nullif(sum({metric}) over (), 0), 1)   as pct_del_total_del_ledger
            from items
            where is_open
            group by lifnr
            order by abs({metric}) desc
            limit ?
            """,
            [_limit(limite)],
        )
    )


def detalle_acreedor(lifnr: str) -> str:
    """Ficha de un acreedor: datos maestros, deuda abierta, aging y comportamiento de pago.

    Args:
        lifnr: Codigo de acreedor (LIFNR). Si el usuario da un nombre, usar antes buscar_acreedor.
    """
    _vendor_exists(lifnr)
    master = _rows(
        """
        select lifnr, vendor_name as nombre, country as pais, purchasing_group as grupo_compras,
               payment_terms as condicion_pago, agreed_payment_days as dias_pactados
        from vendors where lifnr = ?
        """,
        [lifnr],
    )[0]
    totals = _rows(
        """
        select
            count(*)                                                            as documentos,
            count_if(is_open)                                                   as partidas_abiertas,
            round(sum(case when is_open then dmbtr else 0 end), 2)              as saldo_abierto_ars,
            round(sum(case when is_open and days_overdue > 0 then dmbtr else 0 end), 2)
                                                                                as saldo_vencido_ars,
            round(sum(coalesce(fx_revaluation_local, 0)), 2)                    as revaluacion_fx_ars,
            min(bldat)                                                          as primer_documento,
            max(bldat)                                                          as ultimo_documento
        from items where lifnr = ?
        """,
        [lifnr],
    )[0]
    payments = _rows(
        """
        select
            count(*)                                                            as partidas_pagadas,
            round(avg(datediff('day', bldat, augdt)), 1)                        as dias_promedio_de_pago,
            round(avg(days_overdue), 1)                                         as dias_promedio_vs_vencimiento,
            round(100.0 * count_if(is_within_terms) / count(*), 1)              as pct_pagadas_en_termino
        from items where lifnr = ? and not is_open
        """,
        [lifnr],
    )[0]
    return _json(
        {
            "acreedor": master,
            "deuda": totals,
            "aging_abiertas": json.loads(antiguedad_deuda(lifnr)),
            "pagos_historicos": payments,
        }
    )


def buscar_partidas(
    lifnr: str | None = None,
    estado: Literal["abiertas", "compensadas", "todas"] = "abiertas",
    tramo: Literal["No vencida", "Vence hoy", "1-30", "31-60", "61-90", "90+"] | None = None,
    moneda: Literal["ARS", "USD", "EUR"] | None = None,
    orden: Literal[
        "dias_vencido", "importe_ars", "importe_moneda_doc", "fecha_documento"
    ] = "dias_vencido",
    limite: int = 20,
) -> str:
    """Lista partidas individuales con filtros. Devuelve tambien cuantas cumplen el filtro en total.

    Args:
        lifnr: Codigo de acreedor para filtrar. Omitir para todos.
        estado: abiertas (pendientes de pago), compensadas (pagadas) o todas.
        tramo: Tramo de antiguedad para filtrar. Omitir para todos.
        moneda: Moneda del documento (ARS, USD o EUR). Omitir para todas.
        orden: dias_vencido (las mas atrasadas primero), importe_ars (mayor monto en pesos primero), importe_moneda_doc (mayor monto en la moneda del documento primero; usar este para "las mas grandes en dolares") o fecha_documento (las mas recientes primero).
        limite: Cuantas partidas devolver, entre 1 y 100.
    """
    conditions: list[str] = []
    params: list[Any] = []
    if estado == "abiertas":
        conditions.append("is_open")
    elif estado == "compensadas":
        conditions.append("not is_open")
    if lifnr is not None:
        _vendor_exists(lifnr)
        conditions.append("lifnr = ?")
        params.append(lifnr)
    if tramo is not None:
        if tramo not in AGING_BUCKETS:
            raise ToolError(f"tramo invalido: {tramo!r}")
        conditions.append("aging_bucket = ?")
        params.append(tramo)
    if moneda is not None:
        conditions.append("waers = ?")
        params.append(moneda)
    where = " and ".join(conditions) or "true"
    order = {
        "dias_vencido": "days_overdue desc",
        "importe_ars": "abs(dmbtr) desc",
        "importe_moneda_doc": "abs(wrbtr) desc",
        "fecha_documento": "bldat desc",
    }[orden]

    total = _rows(f"select count(*) as n from items where {where}", params)[0]["n"]
    rows = _rows(
        f"""
        select belnr, lifnr, vendor_name as acreedor, blart as clase,
               bldat as fecha_documento, netdt as vencimiento, augdt as fecha_pago,
               waers as moneda, wrbtr as importe_moneda_doc, dmbtr as importe_ars,
               days_overdue as dias_vencido, aging_bucket as tramo
        from items
        where {where}
        order by {order}, belnr
        limit ?
        """,
        [*params, _limit(limite)],
    )
    return _json({"total_que_cumplen_el_filtro": total, "devueltas": len(rows), "partidas": rows})


def consultar_documento(belnr: str) -> str:
    """Detalle completo de un documento por su numero (BELNR).

    Args:
        belnr: Numero de documento SAP, por ejemplo "1000195589".
    """
    rows = _rows(
        """
        select belnr, kidno, xblnr as referencia, lifnr, vendor_name as acreedor,
               blart as clase, document_type_description as tipo_documento,
               bldat as fecha_documento, budat as fecha_contabilizacion, netdt as vencimiento,
               augdt as fecha_pago, augbl as documento_compensacion,
               is_open as abierta, days_overdue as dias_vencido, aging_bucket as tramo,
               waers as moneda, wrbtr as importe_moneda_doc, dmbtr as importe_ars,
               fx_rate_at_document_date as cotizacion_fecha_documento,
               fx_rate_at_cutoff as cotizacion_fecha_corte,
               amount_local_at_cutoff as importe_ars_a_cotizacion_de_corte,
               fx_revaluation_local as revaluacion_fx_ars,
               usnam as usuario
        from items where belnr = ?
        """,
        [belnr.strip()],
    )
    if not rows:
        raise ToolError(f"No existe el documento {belnr!r} en el ledger.")
    return _json(rows[0])


def exposicion_cambiaria() -> str:
    """Exposicion cambiaria de las partidas abiertas en moneda extranjera, por moneda.

    Compara lo contabilizado en pesos a la cotizacion del documento contra lo que
    representa la misma deuda a la cotizacion de la fecha de corte. La diferencia
    (revaluacion) negativa significa que la deuda en pesos crecio: es perdida.
    """
    return _json(
        _rows(
            """
            select
                waers                                           as moneda,
                count(*)                                        as partidas_abiertas,
                round(sum(wrbtr), 2)                            as importe_moneda_doc,
                round(sum(dmbtr), 2)                            as contabilizado_ars,
                round(sum(amount_local_at_cutoff), 2)           as a_cotizacion_de_corte_ars,
                round(sum(fx_revaluation_local), 2)             as revaluacion_ars,
                round(100.0 * sum(fx_revaluation_local)
                      / nullif(sum(sum(fx_revaluation_local)) over (), 0), 1)
                                                                as pct_de_la_revaluacion,
                max(fx_rate_at_cutoff)                          as cotizacion_de_corte
            from items
            where is_open and waers != hwaer
            group by waers
            order by abs(sum(fx_revaluation_local)) desc
            """
        )
    )


_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def comportamiento_pago(lifnr: str | None = None, desde_mes: str | None = None) -> str:
    """Comportamiento de pago por mes de compensacion: cuanto se pago y con que demora.

    Args:
        lifnr: Codigo de acreedor para filtrar. Omitir para todos.
        desde_mes: Primer mes a incluir, formato YYYY-MM. Omitir para toda la historia.
    """
    conditions = ["not is_open"]
    params: list[Any] = []
    if lifnr is not None:
        _vendor_exists(lifnr)
        conditions.append("lifnr = ?")
        params.append(lifnr)
    if desde_mes is not None:
        if not _MONTH.match(desde_mes):
            raise ToolError(f"desde_mes tiene que tener formato YYYY-MM, llego {desde_mes!r}")
        conditions.append("strftime(augdt, '%Y-%m') >= ?")
        params.append(desde_mes)
    return _json(
        _rows(
            f"""
            select
                strftime(augdt, '%Y-%m')                                as mes,
                count(*)                                                as partidas_pagadas,
                round(sum(dmbtr), 2)                                    as importe_ars,
                round(avg(datediff('day', bldat, augdt)), 1)            as dias_promedio_de_pago,
                round(avg(days_overdue), 1)                             as dias_promedio_vs_vencimiento,
                round(100.0 * count_if(is_within_terms) / count(*), 1)  as pct_en_termino
            from items
            where {" and ".join(conditions)}
            group by 1
            order by 1
            """,
            params,
        )
    )


FUNCTIONS = [
    resumen_ledger,
    antiguedad_deuda,
    buscar_acreedor,
    ranking_acreedores,
    detalle_acreedor,
    buscar_partidas,
    consultar_documento,
    exposicion_cambiaria,
    comportamiento_pago,
]

def _audited(fn: Callable[..., str]) -> Callable[..., str]:
    """Registra cada llamada del modelo: argumentos, SQL ejecutado y hash del resultado.

    Envuelve solo lo que llama el modelo. Las funciones de FUNCTIONS siguen
    siendo las originales, asi que una herramienta que llama a otra no genera
    una llamada auditada aparte: sus consultas suman a la de afuera.
    functools.wraps conserva firma, anotaciones y docstring, que es de donde
    beta_tool arma el schema.
    """

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> str:
        queries: list[dict[str, Any]] = []
        token = _queries.set(queries)
        started = time.perf_counter()
        try:
            result = fn(*args, **kwargs)
        except ToolError as exc:
            audit.record_call(
                fn.__name__, kwargs, queries, (time.perf_counter() - started) * 1000,
                error=str(exc.content),
            )
            raise
        finally:
            _queries.reset(token)
        audit.record_call(fn.__name__, kwargs, queries, (time.perf_counter() - started) * 1000,
                          result=result)
        return result

    return wrapper


# El orden es fijo a proposito: las definiciones de herramientas son el
# comienzo del prompt cacheado, y cualquier cambio de orden lo invalida.
TOOLS = [beta_tool(_audited(fn)) for fn in FUNCTIONS]
