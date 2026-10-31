"""Definitions del proyecto: assets, checks, job diario, schedule y recursos.

    dagster dev                                     # UI local en http://localhost:3000
    dagster definitions validate -m orchestration.definitions
"""

import dagster as dg
from dagster_dbt import DbtCliResource

from orchestration.checks import raw_freshness, stg_document_key_unique
from orchestration.dbt_assets import DBT_EXECUTABLE, DBT_PROJECT_DIR, ap_dbt_assets, dbt_project
from orchestration.ingestion import fbl1n_extract, raw_fbl1n_change_feed
from orchestration.partitions import daily_partitions
from orchestration.resources import SnowflakeResource

# Todo el grafo, con sus checks: extracto -> RAW -> staging -> intermediate ->
# marts. Una corrida por particion; un backfill es una corrida por rango.
daily_job = dg.define_asset_job(
    name="fbl1n_daily",
    selection=dg.AssetSelection.all(),
    partitions_def=daily_partitions,
    description="Extracto del dia, carga a RAW y dbt build, con todos los checks.",
)

# 06:00 en Buenos Aires para la particion de ayer. Arranca detenido: activarlo
# en la UI de un daemon de Dagster. En este repo lo dispara GitHub Actions
# (.github/workflows/dagster_daily.yml), que no mantiene un daemon corriendo.
daily_schedule = dg.build_schedule_from_partitioned_job(
    daily_job,
    hour_of_day=6,
    minute_of_hour=0,
    name="fbl1n_daily_0600_art",
)

defs = dg.Definitions(
    assets=[fbl1n_extract, raw_fbl1n_change_feed, ap_dbt_assets],
    asset_checks=[raw_freshness, stg_document_key_unique],
    jobs=[daily_job],
    schedules=[daily_schedule],
    resources={
        "snowflake": SnowflakeResource(),
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=DBT_PROJECT_DIR,
            target=dbt_project.target,
            dbt_executable=DBT_EXECUTABLE,
        ),
    },
)
