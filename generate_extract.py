"""Genera el change feed diario de FBL1N en formato xlsx.

El extracto NO es un snapshot: cada corrida emite unicamente las partidas que
cambiaron ese dia. Un documento re-emitido conserva su BELNR y su KIDNO, y esa
estabilidad es la clave sobre la que el merge de la capa mart resuelve el
estado actual.

Composicion del feed de un dia:
  - documentos creados hoy          (20 a 40)
  - partidas compensadas hoy        (20 a 40, re-emitidas completas)
  - correcciones de importe         (~0.5% del feed, mismo BELNR)
  - late-arriving documents         (~2% del feed, BUDAT de 15-45 dias atras)

Idempotencia
------------
El directorio landing/ es la fuente de verdad, no _state.json. Antes de
generar la fecha D el generador reconstruye el universo de documentos vivos
replicando todos los extractos con fecha < D, y siembra el RNG con
(semilla, D). Correr dos veces la misma fecha produce el mismo archivo.
_state.json es un checkpoint que evita el replay completo en la corrida
incremental de todos los dias.

Uso
---
    python generate_extract.py                      # el dia siguiente a last_run_date
    python generate_extract.py --date 2026-09-07    # una fecha puntual
    python generate_extract.py --days 60            # 60 dias consecutivos
    python generate_extract.py --until 2026-11-05   # hasta esa fecha inclusive
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import math
import random
import re
import sys
from pathlib import Path
from typing import Any

import openpyxl
from openpyxl.utils import get_column_letter

import config

log = logging.getLogger("generate_extract")

# --- parametros del proceso de negocio simulado -----------------------------
CREATED_PER_DAY = (20, 40)
CLEARED_PER_DAY = (20, 40)
CORRECTION_SHARE = 0.005          # sobre el total de filas del feed
LATE_ARRIVING_SHARE = 0.02        # idem
LATE_ARRIVING_BUDAT_LAG = (15, 45)

# Ventana de elegibilidad para compensar, en dias respecto de NETDT.
# El extremo negativo admite pagos anticipados: el aging soporta dias negativos.
CLEARING_WINDOW = (-45, 5)
# Cuantas partidas cubre un mismo documento de compensacion (una corrida de pagos).
PAYMENT_RUN_SIZE = (3, 12)

# Distribuciones calibradas contra las 5.000 partidas del extracto inicial.
AMOUNT_LOG_MEAN = 14.967
AMOUNT_LOG_SD = 1.102
AMOUNT_FLOOR = 64_343.37
AMOUNT_CAP = 25_000_000.00
# BUDAT - BLDAT: pesos empiricos para los offsets 0..14.
BUDAT_OFFSETS = tuple(range(15))
BUDAT_WEIGHTS = (874, 362, 456, 511, 542, 492, 474, 372, 313, 215, 174, 97, 56, 31, 16)
NETDT_MEAN, NETDT_SD, NETDT_MIN, NETDT_MAX = 29.5, 5.5, 13, 48
CURRENCY_MIX = (("ARS", 0.70), ("USD", 0.20), ("EUR", 0.10))
# Signo del importe segun la clase de documento: KR/RE acreditan al proveedor.
CREDIT_DOC_TYPES = (("KR", 0.85), ("RE", 0.15))
DEBIT_DOC_TYPES = (("KG", 0.40), ("KZ", 0.60))
P_CREDIT_DOCUMENT = 0.90
CORRECTION_FACTOR = (0.85, 1.15)

FILENAME_DATE_RE = re.compile(r"FBL1N_(\d{8})\.xlsx$", re.IGNORECASE)

INTEGER_COLUMNS = ("LIFNR", "KIDNO", "BELNR", "AUGBL", "VERZN")


# ---------------------------------------------------------------------------
# utilidades
# ---------------------------------------------------------------------------
def extract_date_from_filename(path: Path) -> dt.date:
    """Deriva la fecha del extracto del nombre del archivo."""
    match = FILENAME_DATE_RE.search(path.name)
    if not match:
        raise ValueError(f"Nombre de extracto no reconocido: {path.name}")
    return dt.datetime.strptime(match.group(1), "%Y%m%d").date()


def landing_extracts(landing_dir: Path) -> list[tuple[dt.date, Path]]:
    """Extractos presentes en landing, ordenados cronologicamente."""
    found = []
    for path in landing_dir.glob(config.EXTRACT_FILENAME_GLOB):
        try:
            found.append((extract_date_from_filename(path), path))
        except ValueError:
            log.warning("Ignorado archivo con nombre fuera de contrato: %s", path.name)
    return sorted(found)


def _weighted_choice(rng: random.Random, options: tuple[tuple[Any, float], ...]) -> Any:
    values = [v for v, _ in options]
    weights = [w for _, w in options]
    return rng.choices(values, weights=weights, k=1)[0]


def _stochastic_round(rng: random.Random, value: float) -> int:
    """Redondeo que preserva la media: 0.3 devuelve 1 el 30% de las veces.

    Con un feed de ~60 filas, un 0.5% de correcciones es menos de una fila por
    dia. Redondear al entero mas cercano las eliminaria por completo.
    """
    floor = math.floor(value)
    return floor + (1 if rng.random() < value - floor else 0)


# ---------------------------------------------------------------------------
# estado y universo de documentos
# ---------------------------------------------------------------------------
def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise config.ConfigError(
            f"No existe {path}. El generador continua un ledger existente; "
            "hace falta el estado inicial que acompana al extracto base."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _docs_to_state(universe: dict[int, dict[str, Any]]) -> dict[str, Any]:
    """Serializa el universo en forma compacta: cabecera + filas posicionales."""
    columns = list(config.RAW_COLUMNS)
    rows = [[doc[c] for c in columns] for _, doc in sorted(universe.items())]
    return {"columns": columns, "rows": rows}


def _docs_from_state(blob: dict[str, Any]) -> dict[int, dict[str, Any]]:
    columns = blob["columns"]
    universe: dict[int, dict[str, Any]] = {}
    for row in blob["rows"]:
        doc = dict(zip(columns, row, strict=True))
        universe[int(doc["BELNR"])] = doc
    return universe


def read_extract(path: Path) -> list[dict[str, Any]]:
    """Lee las filas de detalle de un extracto, descartando la fila de totales."""
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = workbook[config.EXCEL_SHEET_NAME]
        rows = list(sheet.iter_rows(values_only=True))
    finally:
        workbook.close()

    if not rows:
        raise ValueError(f"{path.name}: archivo vacio")
    header = tuple(str(c) if c is not None else "" for c in rows[0])
    if header != config.EXCEL_HEADERS:
        raise ValueError(f"{path.name}: encabezados fuera de contrato")

    documents = []
    for row in rows[1:]:
        if row[0] == config.TOTAL_ROW_MARKER:
            continue
        doc: dict[str, Any] = {}
        for column, value in zip(config.RAW_COLUMNS, row, strict=True):
            if isinstance(value, dt.datetime):
                value = value.date().isoformat()
            elif isinstance(value, dt.date):
                value = value.isoformat()
            doc[column] = value
        documents.append(doc)
    return documents


def build_universe(
    landing_dir: Path,
    before: dt.date,
    state: dict[str, Any] | None = None,
) -> dict[int, dict[str, Any]]:
    """Reconstruye el universo de documentos vivos con lo emitido antes de `before`.

    Ultima version gana: es la misma resolucion que aplica el QUALIFY de la
    capa staging sobre RAW. Que el generador y el modelo colapsen el historial
    con la misma regla es lo que hace verificable al pipeline.
    """
    universe: dict[int, dict[str, Any]] = {}
    replay_from = dt.date.min

    if state and state.get("documents"):
        checkpoint = dt.date.fromisoformat(state["last_run_date"])
        if checkpoint < before:
            universe = _docs_from_state(state["documents"])
            replay_from = checkpoint
            log.info("Checkpoint de estado al %s: %d documentos", checkpoint, len(universe))

    for extract_date, path in landing_extracts(landing_dir):
        if not (replay_from < extract_date < before):
            continue
        log.info("Replay de %s", path.name)
        for doc in read_extract(path):
            universe[int(doc["BELNR"])] = doc

    if not universe:
        raise config.ConfigError(
            f"No hay ningun extracto anterior a {before} en {landing_dir}. "
            "El generador continua un ledger existente, no lo crea de cero."
        )
    return universe


# ---------------------------------------------------------------------------
# referencias externas
# ---------------------------------------------------------------------------
def load_fx_rates(path: Path) -> dict[tuple[str, str], float]:
    """Serie FX diaria del seed. El generador nunca inventa un tipo de cambio."""
    rates: dict[tuple[str, str], float] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rates[(row["rate_date"], row["currency"])] = float(row["rate_to_ars"])
    if not rates:
        raise config.ConfigError(f"{path} no tiene cotizaciones")
    return rates


def fx_rate(rates: dict[tuple[str, str], float], on: str, currency: str) -> float:
    try:
        return rates[(on, currency)]
    except KeyError as exc:
        raise config.ConfigError(
            f"No hay cotizacion de {currency} al {on} en {config.FX_RATES_PATH.name}. "
            "Ampliar la serie FX antes de generar mas dias."
        ) from exc


# ---------------------------------------------------------------------------
# generacion de un feed diario
# ---------------------------------------------------------------------------
class FeedGenerator:
    """Emite el change feed de una fecha a partir del universo previo."""

    def __init__(
        self,
        universe: dict[int, dict[str, Any]],
        rates: dict[tuple[str, str], float],
        seed: int,
        steady_state_open: int,
    ) -> None:
        self.universe = universe
        self.rates = rates
        self.seed = seed
        self.steady_state_open = steady_state_open
        # El mix de acreedores y de usuarios se aprende del ledger que continua,
        # en vez de fijarse por separado y divergir con el tiempo.
        self.vendor_weights = self._empirical_mix("LIFNR")
        self.user_weights = self._empirical_mix("USNAM")
        self._next_belnr = max(int(d["BELNR"]) for d in universe.values())
        self._next_kidno = max(int(d["KIDNO"]) for d in universe.values())
        self._next_augbl = max(
            int(d["AUGBL"]) for d in universe.values() if d["AUGBL"] is not None
        )

    def _empirical_mix(self, column: str) -> tuple[tuple[Any, float], ...]:
        counts: dict[Any, int] = {}
        for doc in self.universe.values():
            counts[doc[column]] = counts.get(doc[column], 0) + 1
        return tuple((value, float(count)) for value, count in sorted(counts.items(), key=str))

    # -- piezas de un documento ------------------------------------------
    def _new_belnr(self, rng: random.Random) -> int:
        self._next_belnr += rng.randint(1, 9)
        return self._next_belnr

    def _new_kidno(self, rng: random.Random) -> int:
        self._next_kidno += rng.randint(1, 9)
        return self._next_kidno

    def _new_augbl(self, rng: random.Random) -> int:
        self._next_augbl += rng.randint(1, 9)
        return self._next_augbl

    def _reference(self, rng: random.Random) -> str:
        """XBLNR del proveedor: 14 caracteres, digitos + A + digitos."""
        prefix = str(rng.randint(1000, 99999))
        suffix_len = 13 - len(prefix)
        return f"{prefix}A{rng.randint(0, 10 ** suffix_len - 1):0{suffix_len}d}"

    def _amount_ars(self, rng: random.Random) -> float:
        magnitude = math.exp(rng.gauss(AMOUNT_LOG_MEAN, AMOUNT_LOG_SD))
        return round(min(max(magnitude, AMOUNT_FLOOR), AMOUNT_CAP), 2)

    def _document_type(self, rng: random.Random) -> tuple[str, int]:
        if rng.random() < P_CREDIT_DOCUMENT:
            return _weighted_choice(rng, CREDIT_DOC_TYPES), -1
        return _weighted_choice(rng, DEBIT_DOC_TYPES), 1

    def _net_due_offset(self, rng: random.Random) -> int:
        offset = round(rng.gauss(NETDT_MEAN, NETDT_SD))
        return min(max(offset, NETDT_MIN), NETDT_MAX)

    def _apply_fx(self, doc: dict[str, Any]) -> None:
        """WRBTR se deriva de DMBTR al tipo de cambio de BLDAT.

        DMBTR (moneda local) es el importe de referencia: es el unico que puede
        agregarse cuando el report mezcla monedas.
        """
        rate = fx_rate(self.rates, doc["BLDAT"], doc["WAERS"])
        doc["WRBTR"] = round(doc["DMBTR"] / rate, 2)

    # -- tipos de cambio del feed ----------------------------------------
    def _create_document(
        self, rng: random.Random, run_date: dt.date, late_arriving: bool
    ) -> dict[str, Any]:
        if late_arriving:
            # El documento existe hace semanas y recien hoy llega al extracto:
            # es lo que rompe cualquier filtro incremental por fecha contable.
            budat = run_date - dt.timedelta(days=rng.randint(*LATE_ARRIVING_BUDAT_LAG))
            bldat = budat - dt.timedelta(days=rng.choices(BUDAT_OFFSETS, BUDAT_WEIGHTS)[0])
        else:
            bldat = run_date
            budat = bldat + dt.timedelta(days=rng.choices(BUDAT_OFFSETS, BUDAT_WEIGHTS)[0])
        netdt = bldat + dt.timedelta(days=self._net_due_offset(rng))

        blart, sign = self._document_type(rng)
        doc = {
            "ICON_STATUS": None,
            "LIFNR": _weighted_choice(rng, self.vendor_weights),
            "KIDNO": self._new_kidno(rng),
            "XBLNR": self._reference(rng),
            "BELNR": self._new_belnr(rng),
            "XREF1": None,
            "ZLSCH": None,
            "ZLSPR": None,
            "VERZN": (run_date - netdt).days,
            "WRBTR": None,
            "WAERS": _weighted_choice(rng, CURRENCY_MIX),
            "DMBTR": round(sign * self._amount_ars(rng), 2),
            "HWAER": config.LOCAL_CURRENCY,
            "BLDAT": bldat.isoformat(),
            "BUDAT": budat.isoformat(),
            "NETDT": netdt.isoformat(),
            "AUGDT": None,
            "AUGBL": None,
            "BLART": blart,
            "USNAM": _weighted_choice(rng, self.user_weights),
            "GKONT": config.RECON_ACCOUNT,
        }
        self._apply_fx(doc)
        return doc

    def _eligible_for_clearing(self, run_date: dt.date) -> list[dict[str, Any]]:
        """Partidas abiertas cuyo vencimiento cae dentro de la ventana de pago."""
        low = run_date + dt.timedelta(days=CLEARING_WINDOW[0])
        high = run_date + dt.timedelta(days=CLEARING_WINDOW[1])
        eligible = []
        for _, doc in sorted(self.universe.items()):
            if doc["AUGDT"] is not None:
                continue
            netdt = dt.date.fromisoformat(doc["NETDT"])
            if not (low <= netdt <= high):
                continue
            # AUGDT >= BUDAT: no se compensa un documento aun no contabilizado.
            if dt.date.fromisoformat(doc["BUDAT"]) > run_date:
                continue
            eligible.append(doc)
        return eligible

    def _clear_items(
        self, rng: random.Random, run_date: dt.date, count: int
    ) -> list[dict[str, Any]]:
        eligible = self._eligible_for_clearing(run_date)
        if not eligible:
            log.warning("%s | no hay partidas elegibles para compensar", run_date)
            return []
        count = min(count, len(eligible))

        # Sesgo hacia las mas vencidas: el peso crece con los dias de atraso,
        # sin llegar a cero para las que todavia no vencieron.
        pool = list(eligible)
        pool_weights = [
            1.0 + max((run_date - dt.date.fromisoformat(d["NETDT"])).days, 0) for d in pool
        ]
        chosen: list[dict[str, Any]] = []
        for _ in range(count):
            index = rng.choices(range(len(pool)), weights=pool_weights, k=1)[0]
            chosen.append(pool.pop(index))
            pool_weights.pop(index)

        # Una corrida de pagos genera un unico documento de compensacion para
        # varias partidas: AUGBL se comparte dentro del lote.
        cleared: list[dict[str, Any]] = []
        position = 0
        while position < len(chosen):
            batch = chosen[position : position + rng.randint(*PAYMENT_RUN_SIZE)]
            augbl = self._new_augbl(rng)
            for source in batch:
                doc = dict(source)
                doc["AUGDT"] = run_date.isoformat()
                doc["AUGBL"] = augbl
                doc["VERZN"] = None   # VERZN solo existe mientras la partida esta abierta
                cleared.append(doc)
                self.universe[int(doc["BELNR"])] = doc
            position += len(batch)
        return cleared

    def _correct_amounts(
        self, rng: random.Random, run_date: dt.date, count: int, exclude: set[int]
    ) -> list[dict[str, Any]]:
        """Re-emite documentos existentes con el importe corregido."""
        if count <= 0:
            return []
        candidates = [
            doc
            for belnr, doc in sorted(self.universe.items())
            if belnr not in exclude
            and doc["AUGDT"] is None
            and (run_date - dt.date.fromisoformat(doc["BLDAT"])).days <= 60
        ]
        if not candidates:
            return []

        corrected = []
        for source in rng.sample(candidates, min(count, len(candidates))):
            doc = dict(source)
            factor = rng.uniform(*CORRECTION_FACTOR)
            while abs(factor - 1.0) < 0.02:   # una correccion tiene que mover el importe
                factor = rng.uniform(*CORRECTION_FACTOR)
            doc["DMBTR"] = round(doc["DMBTR"] * factor, 2)
            self._apply_fx(doc)
            doc["VERZN"] = (run_date - dt.date.fromisoformat(doc["NETDT"])).days
            corrected.append(doc)
        return corrected

    # -- feed completo ----------------------------------------------------
    def _clearing_target(self, created: int) -> int:
        """Cuantas partidas compensar para que el backlog no crezca sin control.

        El volumen sigue al de creaciones, con una correccion proporcional al
        desvio del stock de abiertas respecto del estado estacionario.
        """
        open_now = sum(1 for d in self.universe.values() if d["AUGDT"] is None)
        drift = round((open_now - self.steady_state_open) * 0.05)
        target = created + drift
        return min(max(target, CLEARED_PER_DAY[0]), CLEARED_PER_DAY[1])

    def feed_for(self, run_date: dt.date) -> list[dict[str, Any]]:
        rng = random.Random(f"{self.seed}|{run_date.isoformat()}")

        n_created = rng.randint(*CREATED_PER_DAY)
        n_cleared = self._clearing_target(n_created)
        base_rows = n_created + n_cleared
        n_corrections = _stochastic_round(rng, CORRECTION_SHARE * base_rows)
        n_late = min(_stochastic_round(rng, LATE_ARRIVING_SHARE * base_rows), n_created)

        cleared = self._clear_items(rng, run_date, n_cleared)
        touched = {int(d["BELNR"]) for d in cleared}

        created = []
        late_flags = [True] * n_late + [False] * (n_created - n_late)
        rng.shuffle(late_flags)
        for late in late_flags:
            doc = self._create_document(rng, run_date, late)
            self.universe[int(doc["BELNR"])] = doc
            created.append(doc)
            touched.add(int(doc["BELNR"]))

        corrections = self._correct_amounts(rng, run_date, n_corrections, touched)
        for doc in corrections:
            self.universe[int(doc["BELNR"])] = doc

        feed = cleared + created + corrections
        # Un BELNR aparece a lo sumo una vez por archivo: dentro de una corrida
        # no hay ambiguedad de version que resolver.
        by_document = {int(d["BELNR"]): d for d in feed}
        rows = sorted(by_document.values(), key=lambda d: (int(d["LIFNR"]), int(d["BELNR"])))

        log.info(
            "%s | creados=%d (late=%d) compensados=%d correcciones=%d filas=%d abiertas=%d",
            run_date,
            len(created),
            n_late,
            len(cleared),
            len(corrections),
            len(rows),
            sum(1 for d in self.universe.values() if d["AUGDT"] is None),
        )
        return rows


# ---------------------------------------------------------------------------
# escritura del xlsx
# ---------------------------------------------------------------------------
def _cell_value(column: str, value: Any) -> Any:
    if value is None:
        return None
    if column in config.DATE_COLUMNS:
        return dt.datetime.fromisoformat(value)
    if column in config.AMOUNT_COLUMNS:
        return float(value)
    if column in INTEGER_COLUMNS:
        return int(value)
    return value


def write_extract(path: Path, rows: list[dict[str, Any]]) -> float:
    """Escribe el extracto con la fila de totales al pie, como la emite SAP."""
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = config.EXCEL_SHEET_NAME
    sheet.append(list(config.EXCEL_HEADERS))

    for row in rows:
        sheet.append([_cell_value(c, row[c]) for c in config.RAW_COLUMNS])

    total_local = round(sum(float(r["DMBTR"]) for r in rows), 2)
    total_row: list[Any] = [None] * len(config.RAW_COLUMNS)
    total_row[config.COL_INDEX["ICON_STATUS"]] = config.TOTAL_ROW_MARKER
    total_row[config.COL_INDEX["WRBTR"]] = config.TOTAL_ROW_DOC_AMOUNT
    total_row[config.COL_INDEX["DMBTR"]] = total_local
    sheet.append(total_row)

    for column in config.DATE_COLUMNS:
        letter = get_column_letter(config.COL_INDEX[column] + 1)
        for cell in sheet[letter][1:]:
            cell.number_format = "DD.MM.YYYY"

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    workbook.close()
    return total_local


# ---------------------------------------------------------------------------
# orquestacion
# ---------------------------------------------------------------------------
def resolve_dates(args: argparse.Namespace, last_run: dt.date, today: dt.date) -> list[dt.date]:
    if args.date:
        dates = [args.date]
    elif args.until:
        if args.until <= last_run:
            raise config.ConfigError(
                f"--until {args.until} no es posterior a last_run_date ({last_run})."
            )
        span = (args.until - last_run).days
        dates = [last_run + dt.timedelta(days=i) for i in range(1, span + 1)]
    elif args.days:
        dates = [last_run + dt.timedelta(days=i) for i in range(1, args.days + 1)]
    else:
        dates = [last_run + dt.timedelta(days=1)]

    future = [d for d in dates if d > today]
    if future:
        raise config.ConfigError(
            f"El extracto no puede tener fecha futura: {future[0]} > {today}. "
            f"El ledger esta al {last_run}; hoy solo se puede llegar hasta {today}."
        )
    return dates


def run(args: argparse.Namespace) -> int:
    state = load_state(config.STATE_PATH)
    last_run = dt.date.fromisoformat(state["last_run_date"])
    today = dt.date.today()

    dates = resolve_dates(args, last_run, today)
    log.info("Generando %d dia(s): %s -> %s", len(dates), dates[0], dates[-1])

    rates = load_fx_rates(config.FX_RATES_PATH)
    universe = build_universe(config.LANDING_DIR, dates[0], state)
    generator = FeedGenerator(
        universe=universe,
        rates=rates,
        seed=int(state["seed"]),
        steady_state_open=int(state["open_items"]),
    )

    for run_date in dates:
        rows = generator.feed_for(run_date)
        filename = config.EXTRACT_FILENAME_TEMPLATE.format(yyyymmdd=run_date.strftime("%Y%m%d"))
        path = config.LANDING_DIR / filename
        total = write_extract(path, rows)
        log.info("%s | %d filas | total local %.2f ARS", filename, len(rows), total)

        state.update(
            {
                "last_run_date": run_date.isoformat(),
                "last_belnr": max(int(d["BELNR"]) for d in universe.values()),
                "last_kidno": max(int(d["KIDNO"]) for d in universe.values()),
                "last_augbl": max(
                    int(d["AUGBL"]) for d in universe.values() if d["AUGBL"] is not None
                ),
                "open_items": sum(1 for d in universe.values() if d["AUGDT"] is None),
                "total_items": len(universe),
                "documents": _docs_to_state(universe),
            }
        )
        save_state(config.STATE_PATH, state)

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Genera el change feed diario de FBL1N.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--date",
        type=dt.date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="genera el feed de una fecha puntual",
    )
    group.add_argument(
        "--days",
        type=int,
        metavar="N",
        help="genera N dias consecutivos desde last_run_date + 1",
    )
    group.add_argument(
        "--until",
        type=dt.date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="genera hasta esa fecha inclusive",
    )
    parser.add_argument("--verbose", action="store_true", help="log de nivel DEBUG")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    if args.days is not None and args.days < 1:
        raise SystemExit("--days tiene que ser >= 1")
    try:
        return run(args)
    except config.ConfigError as exc:
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
