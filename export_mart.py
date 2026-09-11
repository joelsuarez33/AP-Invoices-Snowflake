"""Materializa los agregados de la mart a JSON estatico para el tablero.

El tablero no consulta Snowflake. Este script corre como ultimo paso del
pipeline, deja los archivos en web/public/data/ y el sitio en Vercel los sirve
como assets estaticos.

Dos razones, en este orden:

  - Costo. Un tablero conectado al warehouse lo despierta cada vez que alguien
    lo abre, y cada resume factura 60 segundos como minimo. Con JSON estatico el
    warehouse se prende una vez por dia y no lo vuelve a tocar nadie.
  - Superficie. No hace falta exponer credenciales de Snowflake a un frontend
    publico ni montar una capa de API en el medio.

El costo de la decision es que el tablero muestra el ledger a la fecha del
ultimo extracto, no en tiempo real. Para cuentas a pagar, que se opera con
cadencia diaria, es la latencia correcta.

Reutiliza la conexion de load.py: mismo usuario de servicio, misma
autenticacion por key-pair, ninguna credencial nueva.

Uso
---
    python export_mart.py
    python export_mart.py --out web/public/data
    python export_mart.py --dry-run       # imprime las consultas sin conectarse
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import logging
import sys
from pathlib import Path
from typing import Any

import config
from load import connect

log = logging.getLogger("export_mart")


def _marts(objects: dict[str, str]) -> str:
    return f"{objects['database']}.MARTS"


def build_queries(objects: dict[str, str]) -> dict[str, str]:
    """Consultas del tablero. Una por archivo JSON de salida.

    Todas las metricas monetarias estan en moneda local: es la unica que se
    puede agregar cuando el ledger mezcla monedas.
    """
    marts = _marts(objects)

    return {
        # --- cabecera: una sola fila con los KPI del ledger ---
        "summary": f"""
            select
                max(ledger_cutoff_date)                                as ledger_cutoff_date,
                count(*)                                               as items_total,
                count_if(is_open)                                      as items_open,
                count_if(not is_open)                                  as items_cleared,
                round(sum(case when is_open then dmbtr else 0 end), 2)  as open_amount_local,
                round(sum(case when is_open and days_overdue > 0
                               then dmbtr else 0 end), 2)              as overdue_amount_local,
                count_if(is_open and days_overdue > 0)                 as items_overdue,
                round(sum(coalesce(fx_revaluation_local, 0)), 2)        as fx_revaluation_local,
                count(distinct lifnr)                                   as vendors_with_items
            from {marts}.fct_ap_open_items
        """,
        # --- aging de partidas abiertas ---
        "aging": f"""
            select
                aging_bucket,
                aging_bucket_sort,
                count(*)                       as items,
                round(sum(dmbtr), 2)           as amount_local,
                round(avg(days_overdue), 1)    as avg_days_overdue
            from {marts}.fct_ap_open_items
            where is_open
            group by aging_bucket, aging_bucket_sort
            order by aging_bucket_sort
        """,
        # --- concentracion por acreedor ---
        "vendors": f"""
            select
                items.lifnr,
                vendor.vendor_name,
                vendor.country,
                vendor.payment_terms,
                count(*)                                          as open_items,
                round(sum(items.dmbtr), 2)                        as open_amount_local,
                round(sum(case when items.days_overdue > 0
                               then items.dmbtr else 0 end), 2)   as overdue_amount_local,
                round(avg(items.days_overdue), 1)                 as avg_days_overdue,
                round(sum(coalesce(items.fx_revaluation_local, 0)), 2) as fx_revaluation_local
            from {marts}.fct_ap_open_items as items
            inner join {marts}.dim_vendor as vendor
                on items.lifnr = vendor.lifnr
            where items.is_open
            group by items.lifnr, vendor.vendor_name, vendor.country, vendor.payment_terms
            order by abs(sum(items.dmbtr)) desc
            limit 25
        """,
        # --- exposicion cambiaria de las partidas abiertas ---
        "fx_exposure": f"""
            select
                waers                                                   as currency,
                count(*)                                                as open_items,
                round(sum(wrbtr), 2)                                    as amount_document_currency,
                round(sum(dmbtr), 2)                                    as amount_local_at_document_date,
                round(sum(amount_local_at_cutoff), 2)                   as amount_local_at_cutoff,
                round(sum(fx_revaluation_local), 2)                     as fx_revaluation_local,
                round(avg(fx_drift_pct), 2)                             as avg_fx_drift_pct,
                max(fx_rate_at_cutoff)                                  as fx_rate_at_cutoff
            from {marts}.fct_ap_open_items
            where is_open
              and waers != hwaer
              and fx_revaluation_local is not null
            group by waers
            order by abs(sum(fx_revaluation_local)) desc
        """,
        # --- comportamiento de pago por mes ---
        "payment_performance": f"""
            select
                clearing_year_month,
                sum(cleared_items)                                      as cleared_items,
                round(sum(cleared_amount_local), 2)                     as cleared_amount_local,
                round(
                    div0(
                        sum(avg_days_to_pay * cleared_items),
                        sum(cleared_items)
                    ), 1
                )                                                       as avg_days_to_pay,
                round(
                    div0(
                        sum(avg_days_vs_net_due * cleared_items),
                        sum(cleared_items)
                    ), 1
                )                                                       as avg_days_vs_net_due,
                round(
                    100.0 * sum(items_within_terms) / nullif(sum(cleared_items), 0), 1
                )                                                       as within_terms_pct
            from {marts}.fct_ap_payment_performance
            group by clearing_year_month
            order by clearing_year_month
        """,
        # --- evolucion diaria del stock de partidas abiertas ---
        "open_items_trend": f"""
            select
                calendar.date_day                                       as as_of_date,
                count(items.belnr)                                      as open_items,
                round(sum(items.dmbtr), 2)                              as open_amount_local
            from {marts}.dim_date as calendar
            inner join {marts}.fct_ap_open_items as items
                on items.budat <= calendar.date_day
                and (items.augdt is null or items.augdt > calendar.date_day)
            where calendar.date_day between
                dateadd('day', -90, (select max(ledger_cutoff_date) from {marts}.fct_ap_open_items))
                and (select max(ledger_cutoff_date) from {marts}.fct_ap_open_items)
            group by calendar.date_day
            order by calendar.date_day
        """,
    }


def _to_jsonable(value: Any) -> Any:
    """Normaliza los tipos que devuelve el conector a JSON."""
    if isinstance(value, decimal.Decimal):
        # float y no str: los consume un grafico, no un libro contable.
        return float(value)
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return value


def fetch(cursor: Any, sql: str) -> list[dict[str, Any]]:
    cursor.execute(sql)
    columns = [c[0].lower() for c in cursor.description]
    return [
        {column: _to_jsonable(value) for column, value in zip(columns, row, strict=True)}
        for row in cursor.fetchall()
    ]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False),
        encoding="utf-8",
    )


def run(args: argparse.Namespace) -> int:
    objects = config.snowflake_objects()
    out_dir = Path(args.out) if args.out else config.WEB_DATA_DIR
    queries = build_queries(objects)

    if args.dry_run:
        for name, sql in queries.items():
            log.info("[dry-run] %s\n%s", name, sql.strip())
        return 0

    # Una sola conexion para todas las consultas: el warehouse se despierta una
    # vez por corrida y cada resume factura 60 segundos completos.
    connection = connect()
    datasets: dict[str, list[dict[str, Any]]] = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"USE WAREHOUSE {config.require_env('SNOWFLAKE_WAREHOUSE')}")
            for name, sql in queries.items():
                rows = fetch(cursor, sql)
                datasets[name] = rows
                log.info("%s | %d fila(s)", name, len(rows))
    finally:
        connection.close()

    for name, rows in datasets.items():
        target = out_dir / f"{name}.json"
        # summary devuelve una sola fila: se exporta como objeto y no como lista
        # de un elemento, que es como lo quiere consumir el frontend.
        payload: Any = rows[0] if name == "summary" and rows else rows
        write_json(target, payload)
        log.info("%s -> %s", name, target)

    summary = datasets.get("summary") or [{}]
    write_json(
        out_dir / "meta.json",
        {
            "generated_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            "ledger_cutoff_date": summary[0].get("ledger_cutoff_date"),
            "datasets": {name: len(rows) for name, rows in datasets.items()},
            "local_currency": config.LOCAL_CURRENCY,
        },
    )
    log.info("Export completo en %s", out_dir)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Exporta los agregados de la mart a JSON estatico."
    )
    parser.add_argument("--out", metavar="DIR", help="directorio de salida")
    parser.add_argument(
        "--dry-run", action="store_true", help="imprime las consultas sin conectarse"
    )
    parser.add_argument("--verbose", action="store_true", help="log de nivel DEBUG")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    try:
        return run(args)
    except config.ConfigError as exc:
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
