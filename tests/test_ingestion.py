"""Logica de ingesta sin Snowflake: plan de extractos, carga idempotente, frescura."""

import datetime as dt
from pathlib import Path

import pytest

import load
from orchestration.ingestion import GENERATE, REUSE, IngestionError, plan_extracts
from orchestration.partitions import dbt_window_vars, freshness_lag_days
from tests.conftest import FakeCursor, FakeWarehouse

D = dt.date


# ---------------------------------------------------------------------------
# plan de extractos
# ---------------------------------------------------------------------------
def test_plan_reuses_archived_and_generates_next_in_order():
    plan = plan_extracts(
        dates=[D(2026, 10, 3), D(2026, 10, 1), D(2026, 10, 2)],
        archived={D(2026, 10, 1)},
        last_run_date=D(2026, 10, 1),
    )
    assert [(p.extract_date, p.action) for p in plan] == [
        (D(2026, 10, 1), REUSE),
        (D(2026, 10, 2), GENERATE),
        (D(2026, 10, 3), GENERATE),
    ]


def test_plan_refuses_to_skip_a_day():
    with pytest.raises(IngestionError, match="2026-10-02"):
        plan_extracts([D(2026, 10, 3)], archived=set(), last_run_date=D(2026, 10, 1))


def test_plan_refuses_a_past_day_without_archive():
    with pytest.raises(IngestionError, match="archivado"):
        plan_extracts([D(2026, 9, 15)], archived=set(), last_run_date=D(2026, 10, 1))


# ---------------------------------------------------------------------------
# carga idempotente por particion
# ---------------------------------------------------------------------------
OBJECTS = {
    "database": "AP_ANALYTICS",
    "schema_raw": "RAW",
    "stage": "STG_FBL1N",
    "file_format": "FF_FBL1N_CSV",
    "raw_table": "FBL1N_ITEMS",
}


def _csv(staging: Path, extract_date: dt.date, rows: int) -> Path:
    path = staging / f"FBL1N_{extract_date:%Y%m%d}.csv"
    path.write_text("H\n" + "x\n" * rows, encoding="utf-8")
    return path


def test_load_partition_deletes_then_copies_in_one_transaction(pipeline_dirs):
    warehouse = FakeWarehouse()
    path = _csv(pipeline_dirs["staging"], D(2026, 10, 7), rows=5)
    result = load.load_partition(FakeCursor(warehouse), OBJECTS, path)

    kinds = [s.split()[0] for s in warehouse.statements]
    assert kinds == ["PUT", "BEGIN", "DELETE", "COPY", "COMMIT"]
    copy = warehouse.statements[3]
    assert "FILES = ('FBL1N_20261007.csv.gz')" in copy and "FORCE = TRUE" in copy
    assert result.rows_loaded == 5


def test_load_partition_twice_keeps_the_same_rows(pipeline_dirs):
    warehouse = FakeWarehouse()
    path = _csv(pipeline_dirs["staging"], D(2026, 10, 7), rows=5)
    load.load_partition(FakeCursor(warehouse), OBJECTS, path)
    second = load.load_partition(FakeCursor(warehouse), OBJECTS, path)
    assert warehouse.rows == {D(2026, 10, 7): 5}
    assert second.rows_deleted == 5


def test_load_partition_rolls_back_when_copy_fails(pipeline_dirs):
    warehouse = FakeWarehouse()
    warehouse.rows[D(2026, 10, 7)] = 5
    path = _csv(pipeline_dirs["staging"], D(2026, 10, 7), rows=5)
    path.unlink()  # el COPY falla: no encuentra el archivo
    with pytest.raises(FileNotFoundError):
        load.load_partition(FakeCursor(warehouse), OBJECTS, path)
    assert warehouse.statements[-1] == "ROLLBACK"
    assert warehouse.rows == {D(2026, 10, 7): 5}  # el DELETE no quedo aplicado


def test_put_overwrites_and_columns_are_added_idempotently():
    assert "OVERWRITE = TRUE" in load.put_statement(Path("FBL1N_20261007.csv"), "STG")
    statements = load.ensure_columns_statements(OBJECTS)
    assert all("ADD COLUMN IF NOT EXISTS" in s for s in statements)
    assert any(s.endswith("BUKRS STRING") for s in statements)


# ---------------------------------------------------------------------------
# frescura y ventana de dbt
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("latest", "today", "lag"),
    [
        (D(2026, 10, 30), D(2026, 10, 31), 0),  # corrio hoy: al dia
        (D(2026, 10, 29), D(2026, 10, 31), 1),  # una corrida de atraso: limite
        (D(2026, 10, 28), D(2026, 10, 31), 2),  # mas de un dia: falla
        (None, D(2026, 10, 31), None),
    ],
)
def test_freshness_lag(latest, today, lag):
    assert freshness_lag_days(latest, today) == lag


def test_dbt_window_vars():
    assert dbt_window_vars(D(2026, 10, 1), D(2026, 10, 3)) == {
        "extract_window_start": "2026-10-01",
        "extract_window_end": "2026-10-03",
    }
