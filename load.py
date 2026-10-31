"""Sube los CSV normalizados al stage interno y los ingesta en RAW.

RAW es append-only. No hay UPDATE, no hay DELETE y no hay deduplicacion en la
ingesta: cada COPY INTO agrega las filas del feed del dia junto con la metadata
de carga. Un mismo BELNR termina con varias filas en RAW -- la version original
y cada re-emision -- y ese historial es lo que hace auditable el merge de la
capa mart. La resolucion a estado actual ocurre en staging, no aca.

Idempotencia por particion
--------------------------
La particion es el dia del extracto (_extract_date). Cargar un dia es, en una
sola transaccion, borrar las filas de ese dia y copiar su archivo con FORCE.
Reprocesar un dia deja exactamente las filas de su extracto, ni una mas: no
depende del load history de COPY INTO, que vence a los 64 dias (ADR-014).

RAW sigue siendo append-only entre dias: una particion solo reemplaza las
filas de su propio extracto, nunca las versiones que trajeron otros dias.

Costo
-----
Una sola conexion por corrida, con un DELETE + COPY por particion: cada
resume del warehouse factura un minimo de 60 segundos, asi que abrir una
conexion por archivo multiplicaria el costo del backfill por la cantidad de
dias.

Uso
---
    python load.py                        # todos los CSV de staging_files/
    python load.py --pattern "staging_files/FBL1N_20260*.csv"
    python load.py --file staging_files/FBL1N_20260907.csv
    python load.py --dry-run              # imprime el SQL sin conectarse
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import config

log = logging.getLogger("load")

# Regex evaluado en Snowflake sobre METADATA$FILENAME para derivar la fecha del
# extracto. Sobrevive al sufijo .gz que agrega AUTO_COMPRESS.
EXTRACT_DATE_EXPR = (
    "TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'FBL1N_([0-9]{8})', 1, 1, 'e', 1), 'YYYYMMDD')"
)


# ---------------------------------------------------------------------------
# autenticacion
# ---------------------------------------------------------------------------
def load_private_key() -> bytes:
    """Devuelve la clave privada RSA en DER PKCS8, como la espera el conector.

    Dos modos, mutuamente excluyentes:
      - SNOWFLAKE_PRIVATE_KEY_PATH: ruta al .p8 en disco (desarrollo local).
      - SNOWFLAKE_PRIVATE_KEY: el PEM completo en una variable (CI, donde el
        secreto viaja por el store de GitHub y nunca toca el filesystem).
    """
    key_path = config.optional_env("SNOWFLAKE_PRIVATE_KEY_PATH")
    key_pem = config.optional_env("SNOWFLAKE_PRIVATE_KEY")

    if key_path and key_pem:
        raise config.ConfigError(
            "SNOWFLAKE_PRIVATE_KEY_PATH y SNOWFLAKE_PRIVATE_KEY estan ambas definidas. "
            "Elegir un unico modo de autenticacion."
        )
    if key_path:
        path = Path(key_path).expanduser()
        if not path.exists():
            raise config.ConfigError(f"No existe la clave privada en {path}")
        material = path.read_bytes()
    elif key_pem:
        # El secreto de GitHub puede llegar con los saltos de linea escapados.
        material = key_pem.replace("\\n", "\n").encode("utf-8")
    else:
        raise config.ConfigError(
            "Falta la clave privada: definir SNOWFLAKE_PRIVATE_KEY_PATH (local) "
            "o SNOWFLAKE_PRIVATE_KEY (CI). La autenticacion es por key-pair, no por password."
        )

    passphrase = config.optional_env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    try:
        private_key = serialization.load_pem_private_key(
            material,
            password=passphrase.encode("utf-8") if passphrase else None,
            backend=default_backend(),
        )
    except (TypeError, ValueError) as exc:
        raise config.ConfigError(
            "No se pudo leer la clave privada. Revisar el formato PEM PKCS8 y la "
            "passphrase (SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)."
        ) from exc

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect() -> Any:
    import snowflake.connector

    objects = config.snowflake_objects()
    account = config.require_env("SNOWFLAKE_ACCOUNT")
    user = config.require_env("SNOWFLAKE_USER")
    log.info("Conectando a %s como %s", account, user)
    return snowflake.connector.connect(
        account=account,
        user=user,
        role=config.require_env("SNOWFLAKE_ROLE"),
        warehouse=config.require_env("SNOWFLAKE_WAREHOUSE"),
        database=objects["database"],
        schema=objects["schema_raw"],
        private_key=load_private_key(),
        client_session_keep_alive=False,
        application="ap_elt_pipeline",
    )


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
def raw_table(objects: dict[str, str]) -> str:
    return f"{objects['database']}.{objects['schema_raw']}.{objects['raw_table']}"


def put_statement(path: Path, stage: str) -> str:
    # Snowflake espera URI con barras normales, tambien en Windows.
    # OVERWRITE = TRUE: reprocesar un dia reemplaza su archivo en el stage. La
    # deduplicacion no depende del stage sino del DELETE por particion.
    uri = path.resolve().as_posix()
    return (
        f"PUT 'file://{uri}' @{stage} "
        "AUTO_COMPRESS = TRUE "
        "OVERWRITE = TRUE "
        "PARALLEL = 4"
    )


def ensure_columns_statements(objects: dict[str, str]) -> list[str]:
    """Agrega a RAW las columnas del contrato que todavia no tiene.

    Es la mitad del manejo de cambios de schema que puede automatizarse sin
    riesgo: agregar una columna STRING no rompe nada aguas abajo. Quitar o
    retipar una columna, en cambio, lo frena el schema check antes de llegar
    aca.
    """
    return [
        f"ALTER TABLE {raw_table(objects)} ADD COLUMN IF NOT EXISTS {column} STRING"
        for column in config.RAW_COLUMNS
    ]


def copy_statement(objects: dict[str, str], staged_file: str) -> str:
    """COPY INTO de un unico archivo, con FORCE: la particion ya se borro antes."""
    business_columns = ",\n".join(f"        {col}" for col in config.RAW_COLUMNS)
    staged_columns = ",\n".join(f"        t.${i + 1}" for i in range(len(config.RAW_COLUMNS)))
    return f"""
COPY INTO {raw_table(objects)} (
{business_columns},
        _SOURCE_FILE,
        _FILE_ROW_NUMBER,
        _LOADED_AT,
        _EXTRACT_DATE
)
FROM (
    SELECT
{staged_columns},
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP(),
        {EXTRACT_DATE_EXPR}
    FROM @{objects['stage']} t
)
FILES = ('{staged_file}')
FILE_FORMAT = (FORMAT_NAME = '{objects['file_format']}')
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE
""".strip()


def delete_partition_statement(objects: dict[str, str]) -> str:
    return f"DELETE FROM {raw_table(objects)} WHERE _EXTRACT_DATE = %(extract_date)s"


@dataclass(frozen=True)
class PartitionLoad:
    extract_date: dt.date
    rows_deleted: int
    rows_loaded: int


def load_partition(cursor: Any, objects: dict[str, str], csv_path: Path) -> PartitionLoad:
    """Carga un dia de forma idempotente: DELETE de la particion + COPY de su archivo.

    El PUT va antes y afuera de la transaccion porque no es transaccional; el
    DELETE y el COPY van juntos: si el COPY falla, el ROLLBACK deja el dia como
    estaba y no queda una particion a medio cargar.
    """
    extract_date = dt.datetime.strptime(staging_date(csv_path), "%Y%m%d").date()
    cursor.execute(put_statement(csv_path, objects["stage"]))
    cursor.fetchall()

    cursor.execute("BEGIN")
    try:
        cursor.execute(delete_partition_statement(objects), {"extract_date": extract_date})
        deleted = cursor.rowcount or 0
        cursor.execute(copy_statement(objects, f"{csv_path.name}.gz"))
        loaded = 0
        for row in cursor.fetchall():
            # Una fila por archivo: (file, status, rows_parsed, rows_loaded, ...)
            if len(row) >= 4:
                loaded += int(row[3] or 0)
        cursor.execute("COMMIT")
    except BaseException:
        cursor.execute("ROLLBACK")
        raise
    return PartitionLoad(extract_date, deleted, loaded)


# ---------------------------------------------------------------------------
# seleccion de archivos
# ---------------------------------------------------------------------------
def staging_date(path: Path) -> str:
    stem = path.name
    if not stem.upper().startswith("FBL1N_"):
        raise ValueError(f"Nombre de CSV fuera de contrato: {stem}")
    return stem.split("_", 1)[1].split(".", 1)[0]


def resolve_inputs(args: argparse.Namespace) -> list[Path]:
    if args.file:
        paths = [Path(f) for f in args.file]
    elif args.pattern:
        paths = list(Path().glob(args.pattern))
    else:
        paths = list(config.STAGING_DIR.glob(config.STAGING_FILENAME_GLOB))
    missing = [p for p in paths if not p.exists()]
    if missing:
        raise config.ConfigError(f"No existen: {', '.join(str(p) for p in missing)}")
    return sorted(paths, key=staging_date)


# ---------------------------------------------------------------------------
# orquestacion
# ---------------------------------------------------------------------------
def run(args: argparse.Namespace) -> int:
    objects = config.snowflake_objects()
    files = resolve_inputs(args)
    if not files:
        log.info("No hay CSV para cargar en %s", config.STAGING_DIR)
        return 0

    log.info("Particiones a cargar: %d (%s -> %s)", len(files), files[0].name, files[-1].name)

    if args.dry_run:
        for statement in ensure_columns_statements(objects):
            log.info("[dry-run] %s", statement)
        for path in files:
            log.info("[dry-run] %s", put_statement(path, objects["stage"]))
            log.info("[dry-run] %s", delete_partition_statement(objects))
            log.info("[dry-run] %s", copy_statement(objects, f"{path.name}.gz"))
        return 0

    # Una sola conexion para todas las particiones: cada resume del warehouse
    # factura 60 segundos como minimo.
    connection = connect()
    try:
        with connection.cursor() as cursor:
            for statement in ensure_columns_statements(objects):
                cursor.execute(statement)
            for path in files:
                result = load_partition(cursor, objects, path)
                log.info(
                    "%s | borradas=%d cargadas=%d",
                    result.extract_date, result.rows_deleted, result.rows_loaded,
                )
    finally:
        connection.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingesta append-only de FBL1N a RAW.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--file", action="append", metavar="PATH", help="CSV puntual (repetible)")
    group.add_argument("--pattern", metavar="GLOB", help="glob de CSV a cargar")
    parser.add_argument(
        "--dry-run", action="store_true", help="imprime el SQL sin conectarse a Snowflake"
    )
    parser.add_argument("--verbose", action="store_true", help="log de nivel DEBUG")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    try:
        return run(args)
    except config.ConfigError as exc:
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
