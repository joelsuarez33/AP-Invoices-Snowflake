"""Fixtures compartidas: extractos sinteticos en disco y un Snowflake falso.

Ningun test se conecta a Snowflake. FakeWarehouse simula lo justo de RAW para
probar la idempotencia: cuenta filas por _extract_date y, como un COPY real
con FORCE, SUMA las filas del archivo a las que ya hubiera. Si la carga no
borrara la particion antes de copiar, los tests verian duplicados.
"""

import datetime as dt
import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import openpyxl
import pytest

import config
import generate_extract
from orchestration.resources import SnowflakeResource


# ---------------------------------------------------------------------------
# extractos
# ---------------------------------------------------------------------------
def make_document(belnr: int, extract_date: dt.date, *, currency: str = "ARS") -> dict[str, Any]:
    """Un documento con todos los campos del contrato v2, en el formato del generador."""
    bldat = extract_date - dt.timedelta(days=3)
    return {
        "ICON_STATUS": None,
        "LIFNR": 2505,
        "KIDNO": 10_000_000_000_000 + belnr,
        "XBLNR": f"REF{belnr}",
        "BELNR": belnr,
        "XREF1": None,
        "ZLSCH": None,
        "ZLSPR": None,
        "VERZN": 2,
        "WRBTR": -1000.0,
        "WAERS": currency,
        "DMBTR": -1000.0,
        "HWAER": "ARS",
        "BLDAT": bldat.isoformat(),
        "BUDAT": extract_date.isoformat(),
        "NETDT": (bldat + dt.timedelta(days=30)).isoformat(),
        "AUGDT": None,
        "AUGBL": None,
        "BLART": "KR",
        "USNAM": "TESTUSER",
        "GKONT": "9975101",
        "BUKRS": "1000",
        "GJAHR": extract_date.year,
        "BUZEI": 1,
    }


def write_valid_extract(landing: Path, extract_date: dt.date, n_documents: int = 3) -> Path:
    """Extracto que cumple el contrato, escrito con el mismo writer del generador."""
    first = int(extract_date.strftime("%Y%m%d")) * 100
    rows = [make_document(first + i, extract_date) for i in range(n_documents)]
    path = landing / config.EXTRACT_FILENAME_TEMPLATE.format(yyyymmdd=extract_date.strftime("%Y%m%d"))
    generate_extract.write_extract(path, rows)
    return path


def write_custom_extract(
    landing: Path,
    extract_date: dt.date,
    *,
    drop: tuple[str, ...] = (),
    extra: dict[str, Any] | None = None,
    override: dict[str, Any] | None = None,
    n_documents: int = 3,
) -> Path:
    """Extracto con el layout alterado: columnas de menos, de mas o con otro tipo."""
    first = int(extract_date.strftime("%Y%m%d")) * 100
    documents = [make_document(first + i, extract_date) for i in range(n_documents)]
    fields = [f for f in config.RAW_COLUMNS if f not in drop]
    headers = [config.EXCEL_HEADERS[config.COL_INDEX[f]] for f in fields] + list(extra or {})

    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = config.EXCEL_SHEET_NAME
    sheet.append(headers)
    for doc in documents:
        doc.update(override or {})
        values = []
        for f in fields:
            value = doc[f]
            if f in config.DATE_COLUMNS and isinstance(value, str):
                value = dt.datetime.fromisoformat(value)
            values.append(value)
        sheet.append(values + list((extra or {}).values()))
    total = [None] * len(headers)
    total[headers.index(config.EXCEL_HEADERS[config.COL_INDEX["ICON_STATUS"]])] = config.TOTAL_ROW_MARKER
    if "WRBTR" in fields:
        total[fields.index("WRBTR")] = config.TOTAL_ROW_DOC_AMOUNT
    if "DMBTR" in fields:
        dmbtr = [d["DMBTR"] for d in documents]
        total[fields.index("DMBTR")] = sum(v for v in dmbtr if isinstance(v, int | float))
    sheet.append(total)

    path = landing / config.EXTRACT_FILENAME_TEMPLATE.format(yyyymmdd=extract_date.strftime("%Y%m%d"))
    workbook.save(path)
    workbook.close()
    return path


@pytest.fixture
def pipeline_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    """landing/ y staging_files/ aislados, con un estado del generador minimo."""
    landing = tmp_path / "landing"
    staging = tmp_path / "staging"
    landing.mkdir()
    staging.mkdir()
    state_path = landing / "_state.json"
    state_path.write_text(json.dumps({"last_run_date": "2026-09-30"}), encoding="utf-8")
    monkeypatch.setattr(config, "LANDING_DIR", landing)
    monkeypatch.setattr(config, "STAGING_DIR", staging)
    monkeypatch.setattr(config, "STATE_PATH", state_path)
    return {"landing": landing, "staging": staging}


# ---------------------------------------------------------------------------
# Snowflake falso
# ---------------------------------------------------------------------------
class FakeWarehouse:
    def __init__(self) -> None:
        self.rows: dict[dt.date, int] = {}
        self.statements: list[str] = []
        self._snapshot: dict[dt.date, int] | None = None


WAREHOUSE = FakeWarehouse()


class FakeCursor:
    def __init__(self, warehouse: FakeWarehouse) -> None:
        self.warehouse = warehouse
        self.rowcount = 0
        self._result: list[tuple[Any, ...]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        w = self.warehouse
        statement = " ".join(sql.split())
        w.statements.append(statement)
        self._result = []
        if statement == "BEGIN":
            w._snapshot = dict(w.rows)
        elif statement == "ROLLBACK":
            w.rows = w._snapshot or w.rows
        elif statement.startswith("DELETE FROM"):
            self.rowcount = w.rows.pop(params["extract_date"], 0)
        elif statement.startswith("COPY INTO"):
            staged = re.search(r"FILES = \('([^']+)'\)", statement).group(1)
            extract_date = dt.datetime.strptime(re.search(r"(\d{8})", staged).group(1), "%Y%m%d").date()
            csv_path = config.STAGING_DIR / staged.removesuffix(".gz")
            loaded = sum(1 for _ in csv_path.open(encoding="utf-8")) - 1
            # FORCE: el COPY suma lo del archivo a lo que hubiera.
            w.rows[extract_date] = w.rows.get(extract_date, 0) + loaded
            self._result = [(staged, "LOADED", loaded, loaded)]
        elif statement.startswith("SELECT _EXTRACT_DATE, COUNT(*)"):
            start, end = params["start"], params["end"]
            self._result = [(d, n) for d, n in sorted(w.rows.items()) if start <= d <= end]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._result

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._result[0] if self._result else None


class FakeConnection:
    def __init__(self, warehouse: FakeWarehouse) -> None:
        self.warehouse = warehouse

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.warehouse)


class FakeSnowflakeResource(SnowflakeResource):
    @contextmanager
    def get_connection(self) -> Iterator[FakeConnection]:
        yield FakeConnection(WAREHOUSE)

    def objects(self) -> dict[str, str]:
        return {
            "database": "AP_ANALYTICS",
            "schema_raw": "RAW",
            "schema_analytics": "ANALYTICS",
            "stage": "STG_FBL1N",
            "file_format": "FF_FBL1N_CSV",
            "raw_table": "FBL1N_ITEMS",
        }


@pytest.fixture
def warehouse() -> FakeWarehouse:
    WAREHOUSE.rows.clear()
    WAREHOUSE.statements.clear()
    return WAREHOUSE
