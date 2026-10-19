"""Capa de datos del agente: el ledger al ultimo corte, en DuckDB y sin Snowflake.

El estado vigente del ledger ya vive fuera del warehouse. Cada corrida del
pipeline publica en la rama artifacts un _state.json con la ultima version de
cada documento -- lo mismo que resuelve el QUALIFY de stg_fbl1n__items -- y
los maestros son los seeds del repo. Este modulo carga las dos cosas en una
base DuckDB en memoria y reconstruye encima lo que el agente necesita de la
mart: aging, revaluacion cambiaria y totales.

Las reglas son las de los modelos dbt, reescritas en SQL de DuckDB:
  - la antiguedad se mide contra la fecha de corte del ledger, nunca contra hoy;
  - toda agregacion monetaria va en DMBTR;
  - la revaluacion cambiaria es solo de abiertas en moneda extranjera, a la
    ultima cotizacion en o antes del corte.

Reescribir reglas tiene un riesgo: que diverjan. reconcile() lo controla contra
web/public/data/summary.json, que export_mart.py genero desde Snowflake al
mismo corte. Si no cuadra al centavo, la capa local no es fiel y hay que mirar
antes de confiar en lo que responda el agente.

Uso
---
    python -m agent.ledger             # carga, reconcilia y muestra el resumen
    python -m agent.ledger --refresh   # vuelve a traer el estado desde artifacts
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SEEDS_DIR = REPO_ROOT / "dbt_ap" / "seeds"
DATA_DIR = Path(__file__).resolve().parent / "data"
STATE_CACHE = DATA_DIR / "ledger_state.json"
SUMMARY_PATH = REPO_ROOT / "web" / "public" / "data" / "summary.json"

# Proyeccion local: el generador corrido en agent/data/landing desde el estado
# de artifacts hasta ayer, igual que lo haria el pipeline. Sirve mientras el
# pipeline no pueda publicar (por ejemplo, con Snowflake suspendido): el
# generador es determinista, asi que lo que se proyecta aca es exactamente lo
# que el pipeline va a generar y cargar cuando vuelva a correr.
PROJECTION_DIR = DATA_DIR / "landing"
PROJECTION_STATE = PROJECTION_DIR / "_state.json"

# Argentina no tiene horario de verano: UTC-3 todo el ano.
AR_TZ = dt.timezone(dt.timedelta(hours=-3))

ARTIFACTS_REF = "origin/artifacts"
STATE_IN_ARTIFACTS = "state/_state.json"

# Mismo criterio de cuadratura que el checksum de normalize.py y el test
# assert_mart_reconciles_with_raw: un centavo.
TOLERANCE = 0.01

AGING_BUCKETS = ("No vencida", "Vence hoy", "1-30", "31-60", "61-90", "90+")


class LedgerError(RuntimeError):
    """No se pudo armar la capa de datos local."""


# ---------------------------------------------------------------------------
# estado
# ---------------------------------------------------------------------------
def fetch_state(refresh: bool = False) -> dict[str, Any]:
    """Estado mas reciente disponible: el de artifacts o la proyeccion local.

    `_source` queda en el dict para saber de que archivo salio, y con eso
    calcular la huella del dataset.
    """
    state = _artifacts_state(refresh)
    state["_source"] = str(STATE_CACHE)
    if PROJECTION_STATE.exists():
        projected = json.loads(PROJECTION_STATE.read_text(encoding="utf-8"))
        if projected["last_run_date"] > state["last_run_date"]:
            projected["_source"] = str(PROJECTION_STATE)
            return projected
    return state


def advance() -> str:
    """Avanza la proyeccion local hasta ayer, con los mismos pasos que el pipeline.

    Actualiza las cotizaciones del BCRA, parte del estado de artifacts si el
    pipeline esta mas adelantado que la proyeccion, y corre el generador.
    Devuelve el ultimo dia generado.
    """
    subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "update_fx_rates.py")], check=True)

    published = _artifacts_state(refresh=True)
    PROJECTION_DIR.mkdir(parents=True, exist_ok=True)
    projected_date = (
        json.loads(PROJECTION_STATE.read_text(encoding="utf-8"))["last_run_date"]
        if PROJECTION_STATE.exists()
        else ""
    )
    if projected_date < published["last_run_date"]:
        # El pipeline llego mas lejos que la proyeccion: se descarta y se
        # vuelve a partir del estado publicado.
        for stale in PROJECTION_DIR.glob("FBL1N_*.xlsx"):
            stale.unlink()
        shutil.copyfile(STATE_CACHE, PROJECTION_STATE)
        projected_date = published["last_run_date"]

    target = (dt.datetime.now(AR_TZ).date() - dt.timedelta(days=1)).isoformat()
    if projected_date >= target:
        return projected_date
    subprocess.run(
        [sys.executable, str(REPO_ROOT / "generate_extract.py"), "--until", target],
        check=True,
        cwd=REPO_ROOT,
        env={**os.environ, "LANDING_DIR": str(PROJECTION_DIR)},
    )
    return target


def _artifacts_state(refresh: bool = False) -> dict[str, Any]:
    """Estado publicado por el pipeline: del cache local, o de la rama artifacts.

    El cache existe para que la demo no dependa de git ni de red una vez que
    los datos se bajaron.
    """
    if STATE_CACHE.exists() and not refresh:
        return json.loads(STATE_CACHE.read_text(encoding="utf-8"))

    try:
        subprocess.run(
            ["git", "fetch", "-q", "origin", "artifacts"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
        )
        raw = subprocess.run(
            ["git", "show", f"{ARTIFACTS_REF}:{STATE_IN_ARTIFACTS}"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        if STATE_CACHE.exists():
            return json.loads(STATE_CACHE.read_text(encoding="utf-8"))
        raise LedgerError(
            f"No se pudo leer {STATE_IN_ARTIFACTS} de la rama artifacts y no hay cache en "
            f"{STATE_CACHE}. Hace falta git y acceso al remoto la primera vez."
        ) from exc

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATE_CACHE.write_bytes(raw)
    return json.loads(raw)


# ---------------------------------------------------------------------------
# base local
# ---------------------------------------------------------------------------
def _seed(name: str) -> str:
    return (SEEDS_DIR / f"{name}.csv").as_posix()


def build(state: dict[str, Any]) -> duckdb.DuckDBPyConnection:
    """Arma la base en memoria. La tabla que consultan las herramientas es `items`."""
    con = duckdb.connect(":memory:")

    documents = state["documents"]
    # dtype=object: con el default, pandas convierte a float las columnas
    # enteras que tienen nulos (AUGBL, VERZN) y un AUGBL terminaria en ".0".
    docs = pd.DataFrame(documents["rows"], columns=documents["columns"], dtype=object)
    con.register("docs_raw", docs)

    con.execute(
        "create table ledger_meta as select cast(? as date) as ledger_cutoff_date",
        [state["last_run_date"]],
    )

    # --- maestros: espejo de los modelos stg_ap__* ---
    con.execute(f"""
        create table vendors as
        select
            trim(lifnr)                                               as lifnr,
            trim(vendor_name)                                         as vendor_name,
            trim(country)                                             as country,
            trim(payment_terms)                                       as payment_terms,
            trim(purchasing_group)                                    as purchasing_group,
            try_cast(substr(trim(payment_terms), 2) as integer)       as agreed_payment_days
        from read_csv('{_seed("vendors")}', header = true, all_varchar = true)
    """)
    con.execute(f"""
        create table document_types as
        select
            trim(blart)                                               as blart,
            trim(description)                                         as document_type_description,
            cast(is_invoice as boolean)                               as is_invoice
        from read_csv('{_seed("document_types")}', header = true, all_varchar = true)
    """)
    con.execute(f"""
        create table fx_rates as
        select
            cast(rate_date as date)                                   as rate_date,
            trim(currency)                                            as currency,
            cast(rate_to_ars as decimal(18, 6))                       as rate_to_local
        from read_csv('{_seed("fx_rates")}', header = true, all_varchar = true)
    """)

    # --- partidas: stg + int_ap_items_enriched + int_ap_aging + revaluacion ---
    con.execute("""
        create table items as
        with docs as (
            select
                cast(BELNR as varchar)                                as belnr,
                cast(KIDNO as varchar)                                as kidno,
                cast(LIFNR as varchar)                                as lifnr,
                cast(XBLNR as varchar)                                as xblnr,
                cast(AUGBL as varchar)                                as augbl,
                cast(BLART as varchar)                                as blart,
                cast(WAERS as varchar)                                as waers,
                cast(HWAER as varchar)                                as hwaer,
                cast(WRBTR as decimal(19, 2))                         as wrbtr,
                cast(DMBTR as decimal(19, 2))                         as dmbtr,
                cast(BLDAT as date)                                   as bldat,
                cast(BUDAT as date)                                   as budat,
                cast(NETDT as date)                                   as netdt,
                cast(AUGDT as date)                                   as augdt,
                cast(VERZN as integer)                                as verzn_report_unreliable,
                cast(USNAM as varchar)                                as usnam
            from docs_raw
        ),

        rate_at_cutoff as (
            select fx.currency, fx.rate_to_local as fx_rate_at_cutoff
            from fx_rates as fx
            cross join ledger_meta as meta
            where fx.rate_date <= meta.ledger_cutoff_date
            qualify row_number() over (partition by fx.currency order by fx.rate_date desc) = 1
        ),

        enriched as (
            select
                docs.*,
                docs.augdt is null                                    as is_open,
                vendors.vendor_name,
                vendors.country,
                vendors.payment_terms,
                vendors.agreed_payment_days,
                document_types.document_type_description,
                document_types.is_invoice,
                fx_doc.rate_to_local                                  as fx_rate_at_document_date,
                rate_at_cutoff.fx_rate_at_cutoff,
                meta.ledger_cutoff_date,
                datediff('day', docs.netdt,
                         coalesce(docs.augdt, meta.ledger_cutoff_date)) as days_overdue,
                datediff('day', docs.budat,
                         coalesce(docs.augdt, meta.ledger_cutoff_date)) as days_outstanding
            from docs
            -- inner join, como int_ap_items_enriched: un acreedor o una clase
            -- de documento sin maestro no entra. reconcile() lo detecta.
            inner join vendors
                on docs.lifnr = vendors.lifnr
            inner join document_types
                on docs.blart = document_types.blart
            left join fx_rates as fx_doc
                on docs.waers = fx_doc.currency
                and docs.bldat = fx_doc.rate_date
            left join rate_at_cutoff
                on docs.waers = rate_at_cutoff.currency
            cross join ledger_meta as meta
        )

        select
            enriched.*,
            case
                when days_overdue is null then 'Sin vencimiento'
                when days_overdue < 0     then 'No vencida'
                when days_overdue = 0     then 'Vence hoy'
                when days_overdue <= 30   then '1-30'
                when days_overdue <= 60   then '31-60'
                when days_overdue <= 90   then '61-90'
                else '90+'
            end                                                       as aging_bucket,
            case
                when days_overdue is null then 0
                when days_overdue < 0     then 1
                when days_overdue = 0     then 2
                when days_overdue <= 30   then 3
                when days_overdue <= 60   then 4
                when days_overdue <= 90   then 5
                else 6
            end                                                       as aging_bucket_sort,
            days_overdue <= 0                                         as is_within_terms,
            -- Solo abiertas en moneda extranjera: una compensada ya se pago, y
            -- la moneda local contra si misma no tiene exposicion.
            case when is_open and waers != hwaer
                then cast(wrbtr * fx_rate_at_cutoff as decimal(19, 2))
            end                                                       as amount_local_at_cutoff,
            case when is_open and waers != hwaer
                then cast(wrbtr * fx_rate_at_cutoff - dmbtr as decimal(19, 2))
            end                                                       as fx_revaluation_local
        from enriched
    """)
    con.unregister("docs_raw")
    return con


# ---------------------------------------------------------------------------
# totales y reconciliacion
# ---------------------------------------------------------------------------
SUMMARY_SQL = """
    select
        max(ledger_cutoff_date)                                         as ledger_cutoff_date,
        count(*)                                                        as items_total,
        count_if(is_open)                                               as items_open,
        count_if(not is_open)                                           as items_cleared,
        round(sum(case when is_open then dmbtr else 0 end), 2)          as open_amount_local,
        round(sum(case when is_open and days_overdue > 0
                       then dmbtr else 0 end), 2)                       as overdue_amount_local,
        count_if(is_open and days_overdue > 0)                          as items_overdue,
        round(sum(coalesce(fx_revaluation_local, 0)), 2)                as fx_revaluation_local,
        count(distinct lifnr)                                           as vendors_with_items
    from items
"""


def summary(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    cur = con.execute(SUMMARY_SQL)
    columns = [d[0] for d in cur.description]
    return dict(zip(columns, cur.fetchone(), strict=True))


def documents_in_state(state: dict[str, Any]) -> int:
    return len(state["documents"]["rows"])


def reconcile(con: duckdb.DuckDBPyConnection, state: dict[str, Any]) -> list[dict[str, Any]]:
    """Compara los totales locales contra summary.json exportado desde Snowflake.

    Devuelve una fila por control. Los controles solo tienen sentido al mismo
    corte: si summary.json es de otro dia, se informa y no se compara.
    """
    local = summary(con)
    checks: list[dict[str, Any]] = [
        {
            "control": "documentos del estado que entran a items",
            "local": local["items_total"],
            "referencia": documents_in_state(state),
            "ok": local["items_total"] == documents_in_state(state),
        }
    ]

    if not SUMMARY_PATH.exists():
        checks.append({"control": "summary.json", "local": None, "referencia": None, "ok": False})
        return checks

    reference = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    cutoff = local["ledger_cutoff_date"].isoformat()
    if reference.get("ledger_cutoff_date") != cutoff:
        # No es una falla: la proyeccion local esta mas adelante que el ultimo
        # export de Snowflake, y no hay contra que comparar a este corte.
        checks.append(
            {
                "control": "corte de summary.json (sin referencia al mismo corte)",
                "local": cutoff,
                "referencia": reference.get("ledger_cutoff_date"),
                "ok": None,
            }
        )
        return checks

    for key, value in local.items():
        if key == "ledger_cutoff_date" or key not in reference:
            continue
        ok = abs(float(value) - float(reference[key])) <= TOLERANCE
        checks.append({"control": key, "local": value, "referencia": reference[key], "ok": ok})
    return checks


def load(refresh: bool = False) -> tuple[duckdb.DuckDBPyConnection, dict[str, Any]]:
    state = fetch_state(refresh=refresh)
    return build(state), state


def dataset_fingerprint(state: dict[str, Any]) -> str:
    """SHA-256 del estado y de los seeds: identifica el dataset sobre el que se respondio.

    Si dos respuestas tienen la misma huella, se calcularon sobre exactamente
    los mismos datos, y su SQL tiene que devolver exactamente lo mismo.
    """
    digest = hashlib.sha256()
    for path in [Path(state["_source"]), *sorted(SEEDS_DIR.glob("*.csv"))]:
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# exportacion para verificar a mano
# ---------------------------------------------------------------------------
EXPORT_COLUMNS = (
    "belnr", "lifnr", "vendor_name", "country", "payment_terms", "agreed_payment_days",
    "blart", "document_type_description", "bldat", "budat", "netdt", "augdt", "is_open",
    "days_overdue", "aging_bucket", "is_within_terms", "waers", "wrbtr", "dmbtr", "hwaer",
    "fx_rate_at_document_date", "fx_rate_at_cutoff", "amount_local_at_cutoff",
    "fx_revaluation_local", "ledger_cutoff_date",
)


def export(con: duckdb.DuckDBPyConnection, cutoff: str) -> list[Path]:
    """Exporta la tabla items a Excel y CSV, una fila por documento.

    Es la misma tabla que consultan las herramientas: si un numero del agente
    se reproduce con una tabla dinamica sobre este archivo, la respuesta esta
    respaldada por los datos y no por el modelo.
    """
    frame = con.execute(f"select {', '.join(EXPORT_COLUMNS)} from items order by belnr").df()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stem = f"ledger_{cutoff.replace('-', '')}"
    xlsx = DATA_DIR / f"{stem}.xlsx"
    csv = DATA_DIR / f"{stem}.csv"
    frame.to_excel(xlsx, index=False, sheet_name="items")
    # utf-8-sig: Excel en Windows reconoce los acentos solo con BOM.
    frame.to_csv(csv, index=False, encoding="utf-8-sig")
    return [xlsx, csv]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _fmt(value: Any) -> str:
    if isinstance(value, Decimal | float):
        return f"{value:,.2f}"
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Carga y reconcilia el ledger local del agente.")
    parser.add_argument("--refresh", action="store_true", help="vuelve a traer el estado de artifacts")
    parser.add_argument(
        "--avanzar",
        action="store_true",
        help="actualiza cotizaciones y genera en local hasta ayer, como el pipeline",
    )
    parser.add_argument("--export", action="store_true", help="exporta la tabla items a Excel y CSV")
    parser.add_argument("--sql", help="ejecuta una consulta sobre la base local (tabla items)")
    parser.add_argument("--sql-file", type=Path, help="idem, leyendo la consulta de un archivo")
    args = parser.parse_args(argv)

    if args.avanzar:
        try:
            print(f"Proyeccion local al {advance()}.\n")
        except (subprocess.CalledProcessError, LedgerError) as exc:
            print(f"ERROR al avanzar el ledger: {exc}", file=sys.stderr)
            return 1

    try:
        con, state = load(refresh=args.refresh)
    except LedgerError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.sql or args.sql_file:
        query = args.sql or args.sql_file.read_text(encoding="utf-8")
        pd.set_option("display.width", 200)
        pd.set_option("display.max_columns", 30)
        pd.set_option("display.float_format", "{:,.2f}".format)
        print(con.execute(query).df().to_string(index=False))
        return 0

    if args.export:
        for path in export(con, state["last_run_date"]):
            print(f"Exportado: {path}")
        print(f"Huella del dataset: {dataset_fingerprint(state)}")
        return 0

    print(f"Ledger al {state['last_run_date']}: {documents_in_state(state)} documentos en el estado.")
    print(f"Fuente: {state['_source']}\n")
    checks = reconcile(con, state)
    width = max(len(c["control"]) for c in checks)
    for check in checks:
        mark = {True: "OK ", False: "MAL", None: " - "}[check["ok"]]
        print(f"  [{mark}] {check['control']:<{width}}  local={_fmt(check['local'])}  "
              f"referencia={_fmt(check['referencia'])}")

    failed = [c for c in checks if c["ok"] is False]
    print()
    if failed:
        print(f"{len(failed)} control(es) no cuadran: la capa local no es fiel a la mart.")
        return 1
    if any(c["ok"] is None for c in checks):
        print("Sin export de Snowflake a este corte: solo se verifico que todos los documentos entran.")
        return 0
    print("La capa local cuadra con la mart exportada desde Snowflake.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
