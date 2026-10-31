"""Contrato del extracto FBL1N y schema check contra el contrato.

El contrato vive en contracts/fbl1n_extract.yml. Este modulo lo carga y
compara un extracto contra el: que columnas faltan, cuales sobran y que
valores no tienen el tipo declarado. No sabe nada de Snowflake ni de Dagster,
asi que lo usan igual normalize.py, el asset de Dagster y los tests.

Severidad:
  - columna obligatoria faltante, columna duplicada o tipo distinto -> bloquea
  - columna que el contrato no conoce                               -> aviso; se ignora
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

CONTRACT_PATH = Path(__file__).resolve().parent / "contracts" / "fbl1n_extract.yml"

TYPES = ("string", "code", "integer", "decimal", "date")


class ContractDefinitionError(RuntimeError):
    """El YAML del contrato esta mal formado."""


@dataclass(frozen=True)
class Column:
    header: str
    field: str
    type: str
    since: dt.date | None = None

    def required_on(self, extract_date: dt.date) -> bool:
        return self.since is None or extract_date >= self.since


@dataclass(frozen=True)
class Contract:
    version: int
    sheet: str
    columns: tuple[Column, ...]

    @property
    def headers(self) -> tuple[str, ...]:
        return tuple(c.header for c in self.columns)

    @property
    def fields(self) -> tuple[str, ...]:
        return tuple(c.field for c in self.columns)

    def by_header(self) -> dict[str, Column]:
        return {c.header: c for c in self.columns}


def load_contract(path: Path = CONTRACT_PATH) -> Contract:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    columns = []
    for entry in raw["columns"]:
        if entry["type"] not in TYPES:
            raise ContractDefinitionError(
                f"{path.name}: {entry['field']} tiene tipo {entry['type']!r}; validos: {TYPES}"
            )
        since = entry.get("since")
        if isinstance(since, str):
            since = dt.date.fromisoformat(since)
        columns.append(Column(entry["header"], entry["field"], entry["type"], since))
    fields_seen = [c.field for c in columns]
    if len(set(fields_seen)) != len(fields_seen):
        raise ContractDefinitionError(f"{path.name}: hay campos repetidos en el contrato")
    return Contract(version=int(raw["version"]), sheet=raw["sheet"], columns=tuple(columns))


# ---------------------------------------------------------------------------
# tipos
# ---------------------------------------------------------------------------
def value_matches(expected: str, value: Any) -> bool:
    """True si el valor de una celda es compatible con el tipo declarado.

    Las celdas vacias siempre son validas: la obligatoriedad es de la columna,
    no de cada valor (AUGDT esta vacia en toda partida abierta).
    """
    if value is None or value == "":
        return True
    is_number = isinstance(value, int | float) and not isinstance(value, bool)
    if expected == "string":
        return isinstance(value, str)
    if expected == "code":
        return isinstance(value, str) or (is_number and float(value).is_integer())
    if expected == "integer":
        return is_number and float(value).is_integer()
    if expected == "decimal":
        return is_number
    if expected == "date":
        return isinstance(value, dt.date)
    raise ContractDefinitionError(f"tipo desconocido: {expected!r}")


def _type_name(value: Any) -> str:
    if isinstance(value, dt.datetime | dt.date):
        return "date"
    return type(value).__name__


# ---------------------------------------------------------------------------
# schema check
# ---------------------------------------------------------------------------
@dataclass
class SchemaReport:
    extract_date: dt.date
    contract_version: int
    missing: list[str] = field(default_factory=list)
    duplicated: list[str] = field(default_factory=list)
    unexpected: list[str] = field(default_factory=list)
    type_errors: list[dict[str, Any]] = field(default_factory=list)

    @property
    def blocking(self) -> bool:
        return bool(self.missing or self.duplicated or self.type_errors)

    @property
    def has_warnings(self) -> bool:
        return bool(self.unexpected)

    def summary(self) -> str:
        if self.blocking:
            parts = []
            if self.missing:
                parts.append(f"faltan columnas {self.missing}")
            if self.duplicated:
                parts.append(f"columnas duplicadas {self.duplicated}")
            if self.type_errors:
                first = self.type_errors[0]
                parts.append(
                    f"{len(self.type_errors)} columna(s) con tipo distinto, por ejemplo "
                    f"{first['field']}: se esperaba {first['expected']} y en la fila "
                    f"{first['row']} llego {first['found']}"
                )
            return "; ".join(parts)
        if self.unexpected:
            return f"columnas nuevas fuera del contrato, se ignoran: {self.unexpected}"
        return "el extracto cumple el contrato"

    def as_metadata(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "extract_date": self.extract_date.isoformat(),
            "missing_columns": self.missing,
            "duplicated_columns": self.duplicated,
            "unexpected_columns": self.unexpected,
            "type_errors": self.type_errors,
        }


def check_extract(
    header: Sequence[Any],
    rows: Iterable[Sequence[Any]],
    extract_date: dt.date,
    contract: Contract,
) -> SchemaReport:
    """Compara el encabezado y los valores de un extracto contra el contrato.

    `rows` son las filas de detalle, sin la fila de totales. Por cada columna
    con tipo distinto se informa solo la primera fila que falla: alcanza para
    diagnosticar, y un archivo con un layout roto no llena el reporte.
    """
    report = SchemaReport(extract_date=extract_date, contract_version=contract.version)
    names = ["" if h is None else str(h).strip() for h in header]
    known = contract.by_header()

    seen: set[str] = set()
    for name in names:
        if name in seen and name in known:
            report.duplicated.append(known[name].field)
        seen.add(name)
    report.unexpected = [n or "(columna sin encabezado)" for n in names if n not in known]
    report.missing = [
        c.field for c in contract.columns if c.required_on(extract_date) and c.header not in names
    ]

    positions = {name: i for i, name in enumerate(names) if name in known}
    for row_number, row in enumerate(rows, start=2):  # fila 1 = encabezado
        for name, position in positions.items():
            column = known[name]
            if any(e["field"] == column.field for e in report.type_errors):
                continue
            value = row[position] if position < len(row) else None
            if not value_matches(column.type, value):
                report.type_errors.append(
                    {
                        "field": column.field,
                        "expected": column.type,
                        "found": _type_name(value),
                        "row": row_number,
                        "value": str(value)[:40],
                    }
                )
    return report


def to_contract_record(
    header: Sequence[Any], row: Sequence[Any], contract: Contract
) -> dict[str, Any]:
    """Fila del archivo -> dict por campo del contrato, en el orden del contrato.

    Las columnas que el archivo no trae (por ejemplo, anteriores a su `since`)
    quedan en None; las que el contrato no conoce se descartan.
    """
    names = ["" if h is None else str(h).strip() for h in header]
    position = {name: i for i, name in enumerate(names)}
    record = {}
    for column in contract.columns:
        i = position.get(column.header)
        record[column.field] = row[i] if i is not None and i < len(row) else None
    return record
