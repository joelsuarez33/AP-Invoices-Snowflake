"""Borra el esquema de fallas de tests, con todas sus tablas.

dbt crea una tabla por test con store_failures activo, y la reemplaza en cada
corrida. Despues de reducir el alcance de store_failures quedan en el esquema
las tablas de los tests que ya no lo usan: nadie las actualiza y nadie las
borra. Este script las saca de una vez.

No hay nada que recuperar: cada tabla es el resultado del ultimo test que la
escribio, reproducible con `dbt test` o consultando el modelo.

Uso
---
    python scripts/cleanup_test_failures.py              # inventario, no borra
    python scripts/cleanup_test_failures.py --drop       # borra el esquema
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from load import connect  # noqa: E402

log = logging.getLogger("cleanup_test_failures")

# El esquema lo fija dbt_project.yml (data_tests: +schema: test_failures) y
# generate_schema_name lo usa literal, sin prefijo del target.
TEST_FAILURES_SCHEMA = "TEST_FAILURES"


def run(args: argparse.Namespace) -> int:
    objects = config.snowflake_objects()
    database = objects["database"]
    schema = f"{database}.{TEST_FAILURES_SCHEMA}"

    connection = connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                select table_name, row_count, last_altered
                from {database}.information_schema.tables
                where table_schema = '{TEST_FAILURES_SCHEMA}'
                order by last_altered desc
                """
            )
            tables = cursor.fetchall()

            if not tables:
                log.info("%s no tiene tablas (o no existe). Nada que borrar.", schema)
                return 0

            log.info("%s tiene %d tabla(s).", schema, len(tables))
            for name, rows, altered in tables[:10]:
                log.info("  %-45s %6s fila(s)  %s", name, rows, altered)
            if len(tables) > 10:
                log.info("  ... y %d mas", len(tables) - 10)

            if not args.drop:
                log.info("Inventario nada mas. Para borrarlas: --drop")
                return 0

            log.warning("DROP SCHEMA %s CASCADE", schema)
            cursor.execute(f"drop schema if exists {schema} cascade")
            log.info("%s", cursor.fetchone()[0])
            log.info(
                "Listo. La proxima corrida de dbt vuelve a crear el esquema con "
                "las tablas de los tests que si tienen store_failures."
            )
    finally:
        connection.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inventario y borrado del esquema TEST_FAILURES.")
    parser.add_argument(
        "--drop",
        action="store_true",
        help="ejecuta DROP SCHEMA ... CASCADE (sin esta bandera solo lista)",
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
