"""Schema check del extracto contra contracts/fbl1n_extract.yml."""

import csv
import datetime as dt

import pytest

import config
import normalize
from contract import check_extract, load_contract, value_matches
from tests.conftest import write_custom_extract, write_valid_extract

AFTER_V2 = dt.date(2026, 10, 15)
BEFORE_V2 = dt.date(2026, 9, 1)

def test_contract_loads_with_versioned_columns():
    contract = load_contract()
    assert len(contract.columns) == 24
    versioned = {c.field: c.since for c in contract.columns if c.since}
    assert versioned == {f: dt.date(2026, 10, 7) for f in ("BUKRS", "GJAHR", "BUZEI")}
    assert contract.fields == config.RAW_COLUMNS


def test_valid_extract_passes(pipeline_dirs):
    path = write_valid_extract(pipeline_dirs["landing"], AFTER_V2)
    report = normalize.inspect_extract(path).report
    assert not report.blocking
    assert not report.has_warnings


def test_missing_required_column_blocks(pipeline_dirs):
    path = write_custom_extract(pipeline_dirs["landing"], AFTER_V2, drop=("DMBTR",))
    report = normalize.inspect_extract(path).report
    assert report.blocking
    assert report.missing == ["DMBTR"]


def test_columns_before_their_since_are_not_required(pipeline_dirs):
    # Un extracto con el layout v1 es valido antes del 2026-10-07 ...
    old = write_custom_extract(pipeline_dirs["landing"], BEFORE_V2, drop=("BUKRS", "GJAHR", "BUZEI"))
    assert not normalize.inspect_extract(old).report.blocking
    # ... y el mismo layout falla despues.
    new = write_custom_extract(pipeline_dirs["landing"], AFTER_V2, drop=("BUKRS", "GJAHR", "BUZEI"))
    report = normalize.inspect_extract(new).report
    assert report.blocking
    assert report.missing == ["BUKRS", "GJAHR", "BUZEI"]


def test_new_column_is_a_warning_and_ignored(pipeline_dirs):
    path = write_custom_extract(pipeline_dirs["landing"], AFTER_V2, extra={"Centro de costo": "CC01"})
    report = normalize.inspect_extract(path).report
    assert not report.blocking
    assert report.unexpected == ["Centro de costo"]

    target = normalize.normalize_file(path, pipeline_dirs["staging"])
    header = next(csv.reader(target.open(encoding="utf-8")))
    assert tuple(header) == config.RAW_COLUMNS  # la columna nueva no llega al CSV


def test_type_mismatch_blocks(pipeline_dirs):
    path = write_custom_extract(pipeline_dirs["landing"], AFTER_V2, override={"DMBTR": "no-es-un-importe"})
    report = normalize.inspect_extract(path).report
    assert report.blocking
    assert report.type_errors[0]["field"] == "DMBTR"
    assert report.type_errors[0]["expected"] == "decimal"
    assert report.type_errors[0]["row"] == 2


def test_date_column_with_text_blocks():
    contract = load_contract()
    header = list(contract.headers)
    row = [None] * len(header)
    row[header.index("Fecha de documento")] = "06.10.2026"  # texto, no una fecha de Excel
    report = check_extract(header, [row], AFTER_V2, contract)
    assert any(e["field"] == "BLDAT" for e in report.type_errors)


@pytest.mark.parametrize(
    ("expected", "value", "ok"),
    [
        ("code", 9975101, True),
        ("code", "9975101", True),  # GKONT llega como numero o como texto
        ("code", 1.5, False),
        ("integer", 3.0, True),
        ("integer", 3.5, False),
        ("decimal", -1000.25, True),
        ("decimal", "x", False),
        ("date", dt.datetime(2026, 10, 6), True),
        ("date", "2026-10-06", False),
        ("string", None, True),  # vacio siempre es valido
        ("string", True, False),
    ],
)
def test_value_matches(expected, value, ok):
    assert value_matches(expected, value) is ok


def test_normalize_rejects_missing_column(pipeline_dirs):
    path = write_custom_extract(pipeline_dirs["landing"], AFTER_V2, drop=("BELNR",))
    with pytest.raises(normalize.ContractError, match="BELNR"):
        normalize.normalize_file(path, pipeline_dirs["staging"])


def test_v1_extract_normalizes_to_full_width(pipeline_dirs):
    path = write_custom_extract(pipeline_dirs["landing"], BEFORE_V2, drop=("BUKRS", "GJAHR", "BUZEI"))
    target = normalize.normalize_file(path, pipeline_dirs["staging"])
    rows = list(csv.DictReader(target.open(encoding="utf-8")))
    assert set(rows[0]) == set(config.RAW_COLUMNS)
    assert rows[0]["BUKRS"] == "" and rows[0]["GJAHR"] == ""  # las deriva staging
