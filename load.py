"""Sube los CSV normalizados al stage interno y los ingesta en RAW.

RAW es append-only. No hay UPDATE, no hay DELETE y no hay deduplicacion en la
ingesta: cada COPY INTO agrega las filas del feed del dia junto con la metadata
de carga. Un mismo BELNR termina con varias filas en RAW -- la version original
y cada re-emision -- y ese historial es lo que hace auditable el merge de la
capa mart. La resolucion a estado actual ocurre en staging, no aca.

Idempotencia
------------
La provee Snowflake: COPY INTO registra los archivos ya cargados en el load
history del stage y los ignora en corridas posteriores, con una ventana de 64
dias. Por eso el PUT usa OVERWRITE=FALSE y el COPY no lleva FORCE. Implementar
deduplicacion propia seria reescribir peor algo que el motor ya garantiza.

Costo
-----
Una sola conexion y un solo COPY por corrida: cada resume del warehouse
factura un minimo de 60 segundos, asi que abrir una conexion por archivo
multiplicaria el costo del backfill por la cantidad de dias.

Uso
---
    python load.py                        # todos los CSV de staging_files/
    python load.py --pattern "staging_files/FBL1N_20260*.csv"
    python load.py --file staging_files/FBL1N_20260907.csv
    python load.py --dry-run              # imprime el SQL sin conectarse
"""

from __future__ import annotations

import argparse
import logging
import sys
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
def put_statement(path: Path, stage: str) -> str:
    # Snowflake espera URI con barras normales, tambien en Windows.
    uri = path.resolve().as_posix()
    return (
        f"PUT 'file://{uri}' @{stage} "
        "AUTO_COMPRESS = TRUE "
        "OVERWRITE = FALSE "
        "PARALLEL = 4"
    )


def copy_statement(objects: dict[str, str]) -> str:
    """COPY INTO con transformacion: agrega la metadata de carga a cada fila."""
    business_columns = ",\n".join(f"        {col}" for col in config.RAW_COLUMNS)
    staged_columns = ",\n".join(f"        t.${i + 1}" for i in range(len(config.RAW_COLUMNS)))
    target = f"{objects['database']}.{objects['schema_raw']}.{objects['raw_table']}"
    return f"""
COPY INTO {target} (
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
PATTERN = '.*FBL1N_[0-9]{{8}}[.]csv([.]gz)?'
FILE_FORMAT = (FORMAT_NAME = '{objects['file_format']}')
ON_ERROR = 'ABORT_STATEMENT'
""".strip()


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

    log.info("Archivos a ingestar: %d (%s -> %s)", len(files), files[0].name, files[-1].name)
    copy_sql = copy_statement(objects)

    if args.dry_run:
        for path in files:
            log.info("[dry-run] %s", put_statement(path, objects["stage"]))
        log.info("[dry-run] %s", copy_sql)
        return 0

    connection = connect()
    try:
        with connection.cursor() as cursor:
            for path in files:
                cursor.execute(put_statement(path, objects["stage"]))
                for row in cursor.fetchall():
                    # (source, target, source_size, target_size, source_compression,
                    #  target_compression, status, message)
                    log.info("PUT %s -> %s [%s]", row[0], row[1], row[6])

            log.info("COPY INTO %s", objects["raw_table"])
            cursor.execute(copy_sql)
            results = cursor.fetchall()

        if not results:
            log.info("Sin archivos nuevos: el load history del stage ya los registro.")
            return 0

        loaded = 0
        for row in results:
            file_name, status, rows_parsed, rows_loaded = row[0], row[1], row[2], row[3]
            loaded += int(rows_loaded or 0)
            log.info("%s | %s | parsed=%s loaded=%s", file_name, status, rows_parsed, rows_loaded)
        log.info("Filas agregadas a RAW: %d", loaded)
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
