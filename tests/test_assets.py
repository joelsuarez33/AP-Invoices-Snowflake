"""Los assets de ingesta materializados en proceso, con un Snowflake falso.

Prueban lo que piden los criterios de aceptacion sin conectarse a nada:
reprocesar una particion no duplica, un backfill de varios dias tampoco, y un
extracto que rompe el contrato frena la carga.
"""

import datetime as dt

import dagster as dg

from orchestration.ingestion import (
    ROW_COUNT_CHECK,
    SCHEMA_CHECK,
    fbl1n_extract,
    raw_fbl1n_change_feed,
)
from tests.conftest import FakeSnowflakeResource, write_custom_extract, write_valid_extract

ASSETS = [fbl1n_extract, raw_fbl1n_change_feed]
RANGE_START = "dagster/asset_partition_range_start"
RANGE_END = "dagster/asset_partition_range_end"


def _materialize(**kwargs):
    return dg.materialize(
        ASSETS,
        resources={"snowflake": FakeSnowflakeResource()},
        raise_on_error=False,
        **kwargs,
    )


def _checks(result):
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


def _materialized(result):
    return {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}


def test_materializing_a_partition_twice_keeps_the_row_count(pipeline_dirs, warehouse):
    day = dt.date(2026, 10, 1)
    write_valid_extract(pipeline_dirs["landing"], day, n_documents=4)

    first = _materialize(partition_key=day.isoformat())
    second = _materialize(partition_key=day.isoformat())

    assert first.success and second.success
    assert warehouse.rows == {day: 4}
    assert _checks(second)[ROW_COUNT_CHECK].passed


def test_backfill_of_three_days_runs_once_without_duplicates(pipeline_dirs, warehouse):
    days = [dt.date(2026, 10, 1) + dt.timedelta(days=i) for i in range(3)]
    for i, day in enumerate(days):
        write_valid_extract(pipeline_dirs["landing"], day, n_documents=2 + i)
    tags = {RANGE_START: days[0].isoformat(), RANGE_END: days[-1].isoformat()}

    first = _materialize(tags=tags)
    second = _materialize(tags=tags)

    assert first.success and second.success
    assert warehouse.rows == {days[0]: 2, days[1]: 3, days[2]: 4}
    # Una sola carga por dia en cada corrida: tres DELETE y tres COPY.
    assert sum(s.startswith("COPY INTO") for s in warehouse.statements) == 6


def test_missing_column_fails_the_schema_check_and_nothing_is_loaded(pipeline_dirs, warehouse):
    day = dt.date(2026, 10, 15)
    write_custom_extract(pipeline_dirs["landing"], day, drop=("DMBTR",))

    result = _materialize(partition_key=day.isoformat())

    schema = _checks(result)[SCHEMA_CHECK]
    assert not schema.passed
    assert schema.severity == dg.AssetCheckSeverity.ERROR
    assert "raw_fbl1n_change_feed" not in _materialized(result)
    assert warehouse.statements == []  # Snowflake no se toco


def test_new_column_warns_and_still_loads(pipeline_dirs, warehouse):
    day = dt.date(2026, 10, 15)
    write_custom_extract(pipeline_dirs["landing"], day, extra={"Centro de costo": "CC01"})

    result = _materialize(partition_key=day.isoformat())

    schema = _checks(result)[SCHEMA_CHECK]
    assert not schema.passed
    assert schema.severity == dg.AssetCheckSeverity.WARN
    assert "raw_fbl1n_change_feed" in _materialized(result)
    assert warehouse.rows == {day: 3}
