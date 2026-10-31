"""Asset checks que consultan el warehouse.

Los checks del extracto (contrato) y de la carga (filas por particion) los
emite cada asset al materializarse, porque tienen que poder frenar lo que
sigue. Estos dos miran el estado de las tablas despues de la corrida.
"""

import dagster as dg
from dagster_dbt import get_asset_key_for_model

import load
from orchestration.dbt_assets import ap_dbt_assets
from orchestration.partitions import freshness_lag_days, today_in_argentina
from orchestration.resources import SnowflakeResource

# El extracto del dia D se toma el D+1 a la manana. Recien despues de esa
# corrida el ledger esta al dia; un dia mas de atraso es una corrida perdida.
MAX_LAG_DAYS = 1

STG_ITEMS = get_asset_key_for_model([ap_dbt_assets], "stg_fbl1n__items")


@dg.asset_check(
    asset="raw_fbl1n_change_feed",
    description=(
        f"La ultima particion cargada en RAW no tiene mas de {MAX_LAG_DAYS} dia de atraso "
        "respecto de ayer (hora argentina)."
    ),
)
def raw_freshness(snowflake: SnowflakeResource) -> dg.AssetCheckResult:
    objects = snowflake.objects()
    with snowflake.get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(f"SELECT MAX(_EXTRACT_DATE) FROM {load.raw_table(objects)}")
        latest = cursor.fetchone()[0]
    lag = freshness_lag_days(latest, today_in_argentina())
    return dg.AssetCheckResult(
        passed=lag is not None and lag <= MAX_LAG_DAYS,
        description=(
            "RAW vacia" if lag is None else f"ultima particion {latest}, {lag} dia(s) de atraso"
        ),
        metadata={"latest_partition": str(latest), "lag_days": lag if lag is not None else -1},
    )


@dg.asset_check(
    asset=STG_ITEMS,
    description=(
        "Unicidad de la clave del documento (sociedad, numero, ejercicio, posicion) en "
        "stg_fbl1n__items, despues de que el QUALIFY resuelve el change feed."
    ),
)
def stg_document_key_unique(snowflake: SnowflakeResource) -> dg.AssetCheckResult:
    database = snowflake.objects()["database"]
    with snowflake.get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT BUKRS, BELNR, GJAHR, BUZEI
                FROM {database}.STAGING.STG_FBL1N__ITEMS
                GROUP BY BUKRS, BELNR, GJAHR, BUZEI
                HAVING COUNT(*) > 1
            )
            """
        )
        duplicated = int(cursor.fetchone()[0])
    return dg.AssetCheckResult(
        passed=duplicated == 0,
        description=f"{duplicated} clave(s) duplicada(s)",
        metadata={"duplicated_keys": duplicated},
    )
