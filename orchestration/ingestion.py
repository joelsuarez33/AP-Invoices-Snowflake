"""Assets de ingesta: el extracto del dia y su carga a RAW.

Son dos assets y no uno, para que el schema check corra antes de tocar el
warehouse:

    fbl1n_extract          el archivo del dia: lo lee del archivo historico
                           o lo genera. Lleva el schema check, bloqueante.
    raw_fbl1n_change_feed  la carga a RAW: DELETE + COPY de la particion.
                           Lleva el check de filas, bloqueante.

Si el extracto no cumple el contrato, raw_fbl1n_change_feed no corre y dbt
tampoco: un archivo con una columna faltante nunca llega a Snowflake.

Los dos usan BackfillPolicy.single_run: un backfill de N dias es una sola
corrida que procesa los dias en orden. El generador es secuencial -- el feed
del dia D sale del universo que dejo el D-1 --, y una sola corrida enciende
el warehouse una sola vez.
"""

import datetime as dt
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dagster as dg

import config
import generate_extract
import load
import normalize
from orchestration.partitions import daily_partitions, partition_dates
from orchestration.resources import SnowflakeResource

GENERATE = "generar"
REUSE = "reutilizar"


class IngestionError(RuntimeError):
    """No se puede armar el extracto pedido sin romper el orden del change feed."""


@dataclass(frozen=True)
class PlannedExtract:
    extract_date: dt.date
    action: str  # GENERATE | REUSE


def extract_path(extract_date: dt.date) -> Path:
    return config.LANDING_DIR / config.EXTRACT_FILENAME_TEMPLATE.format(
        yyyymmdd=extract_date.strftime("%Y%m%d")
    )


def archived_dates(landing_dir: Path) -> set[dt.date]:
    return {d for d, _ in generate_extract.landing_extracts(landing_dir)}


def plan_extracts(
    dates: list[dt.date], archived: set[dt.date], last_run_date: dt.date
) -> list[PlannedExtract]:
    """Decide, dia por dia, si el extracto se reutiliza o se genera.

    - Un dia que ya tiene archivo se reutiliza: reprocesarlo es volver a
      cargar exactamente el mismo extracto.
    - Un dia sin archivo solo se puede generar si es el siguiente al estado
      del generador. Saltear un dia dejaria el universo de documentos en un
      estado que ningun extracto real produjo.
    """
    planned: list[PlannedExtract] = []
    next_to_generate = last_run_date + dt.timedelta(days=1)
    for extract_date in sorted(dates):
        if extract_date in archived:
            planned.append(PlannedExtract(extract_date, REUSE))
        elif extract_date == next_to_generate:
            planned.append(PlannedExtract(extract_date, GENERATE))
            next_to_generate += dt.timedelta(days=1)
        elif extract_date < next_to_generate:
            raise IngestionError(
                f"No hay extracto archivado del {extract_date} y el generador ya paso ese dia "
                f"(estado al {last_run_date}). Restaurar el archivo en {config.LANDING_DIR} "
                "o reprocesar solo dias archivados."
            )
        else:
            raise IngestionError(
                f"Para generar el {extract_date} falta antes el {next_to_generate}: el change feed "
                "se genera en orden. Materializar el rango desde ese dia."
            )
    return planned


def fx_covers(dates: list[dt.date]) -> bool:
    rates = generate_extract.load_fx_rates(config.FX_RATES_PATH)
    return all(
        (d.isoformat(), currency) in rates for d in dates for currency in ("USD", "EUR")
    )


def ensure_fx_rates(dates: list[dt.date], log: Any) -> None:
    """Completa la serie del BCRA si no cubre los dias a generar.

    Si la API no responde (desde los runners de GitHub devuelve 403), alcanza
    con que el seed commiteado cubra los dias. Si no los cubre, se corta aca y
    no se genera nada con una serie incompleta.
    """
    if fx_covers(dates):
        return
    script = config.REPO_ROOT / "scripts" / "update_fx_rates.py"
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    log.info(result.stdout.strip() or result.stderr.strip())
    if not fx_covers(dates):
        raise dg.Failure(
            description=(
                f"El seed de cotizaciones no cubre hasta el {max(dates)} y la API del BCRA no "
                "respondio. Correr `python scripts/update_fx_rates.py` desde una red con acceso "
                "y commitear dbt_ap/seeds/fx_rates.csv."
            )
        )


# ---------------------------------------------------------------------------
# assets
# ---------------------------------------------------------------------------
SCHEMA_CHECK = "schema_contract"
ROW_COUNT_CHECK = "partition_row_count"


@dg.asset(
    partitions_def=daily_partitions,
    backfill_policy=dg.BackfillPolicy.single_run(),
    group_name="ingestion",
    kinds={"python", "excel"},
    check_specs=[
        dg.AssetCheckSpec(
            SCHEMA_CHECK,
            asset="fbl1n_extract",
            blocking=True,
            description=(
                "Columnas y tipos del extracto contra contracts/fbl1n_extract.yml. Columna "
                "faltante o tipo distinto: falla y frena la carga. Columna nueva: aviso, se ignora."
            ),
        )
    ],
    description="Extracto FBL1N del dia (change feed): archivado o generado en orden.",
)
def fbl1n_extract(context: dg.AssetExecutionContext) -> Iterator[Any]:
    dates = partition_dates(context)
    state = generate_extract.load_state(config.STATE_PATH)
    last_run = dt.date.fromisoformat(state["last_run_date"])
    try:
        plan = plan_extracts(dates, archived_dates(config.LANDING_DIR), last_run)
    except IngestionError as exc:
        raise dg.Failure(description=str(exc)) from exc

    to_generate = [p.extract_date for p in plan if p.action == GENERATE]
    if to_generate:
        ensure_fx_rates(to_generate, context.log)
        generate_extract.generate(to_generate, state)
    context.log.info(
        "Extractos: %d reutilizados, %d generados",
        len(plan) - len(to_generate), len(to_generate),
    )

    reports = [normalize.inspect_extract(extract_path(d)).report for d in dates]
    blocking = [r for r in reports if r.blocking]
    warnings = [r for r in reports if r.has_warnings]
    if blocking:
        passed, severity = False, dg.AssetCheckSeverity.ERROR
        summary = "; ".join(f"{r.extract_date}: {r.summary()}" for r in blocking)
    elif warnings:
        passed, severity = False, dg.AssetCheckSeverity.WARN
        summary = "; ".join(f"{r.extract_date}: {r.summary()}" for r in warnings)
    else:
        passed, severity, summary = True, dg.AssetCheckSeverity.ERROR, "cumple el contrato"

    yield dg.MaterializeResult(
        metadata={
            "dates": [d.isoformat() for d in dates],
            "generated": [d.isoformat() for d in to_generate],
            "landing_dir": str(config.LANDING_DIR),
        },
        check_results=[
            dg.AssetCheckResult(
                check_name=SCHEMA_CHECK,
                passed=passed,
                severity=severity,
                description=summary,
                metadata={"reports": [r.as_metadata() for r in reports]},
            )
        ],
    )


@dg.asset(
    deps=[fbl1n_extract],
    partitions_def=daily_partitions,
    backfill_policy=dg.BackfillPolicy.single_run(),
    group_name="ingestion",
    kinds={"snowflake"},
    check_specs=[
        dg.AssetCheckSpec(
            ROW_COUNT_CHECK,
            asset="raw_fbl1n_change_feed",
            blocking=True,
            description="Cada particion cargada tiene al menos una fila en RAW.",
        )
    ],
    description=(
        "RAW.FBL1N_ITEMS: append-only entre dias, idempotente por dia (DELETE de la "
        "particion + COPY de su archivo, en una transaccion)."
    ),
)
def raw_fbl1n_change_feed(
    context: dg.AssetExecutionContext, snowflake: SnowflakeResource
) -> Iterator[Any]:
    dates = partition_dates(context)
    try:
        csv_paths = [normalize.normalize_file(extract_path(d), config.STAGING_DIR) for d in dates]
    except normalize.ContractError as exc:
        raise dg.Failure(description=str(exc)) from exc

    objects = snowflake.objects()
    with snowflake.get_connection() as connection, connection.cursor() as cursor:
        for statement in load.ensure_columns_statements(objects):
            cursor.execute(statement)
        loads = [load.load_partition(cursor, objects, path) for path in csv_paths]
        # El conteo sale de la tabla despues de la carga, no del resultado del
        # COPY: es lo que va a ver dbt.
        cursor.execute(
            f"SELECT _EXTRACT_DATE, COUNT(*) FROM {load.raw_table(objects)} "
            "WHERE _EXTRACT_DATE BETWEEN %(start)s AND %(end)s GROUP BY 1",
            {"start": dates[0], "end": dates[-1]},
        )
        counts = {
            (d if isinstance(d, dt.date) else dt.date.fromisoformat(str(d))): int(n)
            for d, n in cursor.fetchall()
        }

    rows_per_day = {d.isoformat(): counts.get(d, 0) for d in dates}
    empty = [day for day, rows in rows_per_day.items() if rows == 0]
    for result in loads:
        context.log.info(
            "%s | reemplazadas=%d cargadas=%d", result.extract_date, result.rows_deleted,
            result.rows_loaded,
        )
    yield dg.MaterializeResult(
        metadata={
            "rows_per_partition": rows_per_day,
            "rows_loaded": sum(r.rows_loaded for r in loads),
            "rows_replaced": sum(r.rows_deleted for r in loads),
        },
        check_results=[
            dg.AssetCheckResult(
                check_name=ROW_COUNT_CHECK,
                passed=not empty,
                description=f"particiones vacias: {empty}" if empty else "todas con filas",
                metadata={"rows_per_partition": rows_per_day},
            )
        ],
    )
