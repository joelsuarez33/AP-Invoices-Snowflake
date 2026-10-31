"""Particion diaria del pipeline: un dia = un extracto FBL1N = un _extract_date."""

from __future__ import annotations

import datetime as dt

import dagster as dg

TIMEZONE = "America/Argentina/Buenos_Aires"

# Primer extracto del ledger (el baseline de scripts/enrich_baseline.py).
FIRST_EXTRACT_DATE = "2026-07-08"

# La particion del dia D existe recien cuando D termina en hora argentina: el
# extracto de un dia se toma a la manana del siguiente. Por eso la ultima
# particion disponible es siempre ayer, igual que el tope del generador.
daily_partitions = dg.DailyPartitionsDefinition(
    start_date=FIRST_EXTRACT_DATE,
    timezone=TIMEZONE,
)


def partition_dates(context: dg.AssetExecutionContext | dg.AssetCheckExecutionContext) -> list[dt.date]:
    """Dias que cubre la corrida, en orden: uno en la corrida diaria, N en un backfill."""
    return sorted(dt.date.fromisoformat(key) for key in context.partition_keys)


def today_in_argentina() -> dt.date:
    from zoneinfo import ZoneInfo

    return dt.datetime.now(ZoneInfo(TIMEZONE)).date()


def freshness_lag_days(latest_partition: dt.date | None, today: dt.date) -> int | None:
    """Dias de atraso de la ultima particion cargada respecto de ayer.

    El extracto del dia D se toma el D+1 a la manana: con la corrida de hoy
    hecha, la ultima particion es ayer y el atraso es 0.
    """
    if latest_partition is None:
        return None
    return (today - dt.timedelta(days=1) - latest_partition).days


def dbt_window_vars(start: dt.date, end: dt.date) -> dict[str, str]:
    """Vars que recibe fct_ap_open_items para filtrar el incremental por la ventana."""
    return {"extract_window_start": start.isoformat(), "extract_window_end": end.isoformat()}
