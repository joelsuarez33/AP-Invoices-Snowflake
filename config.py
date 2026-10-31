"""Contrato unico del pipeline: layout del extracto, rutas y conexion.

Todo lo que describe la forma del archivo FBL1N vive aca. Los cuatro scripts
del pipeline importan de este modulo en lugar de repetir literales, para que
un cambio de variante de layout en SAP se refleje en un solo lugar.

Ninguna credencial se define aca. Los valores sensibles se leen de entorno y
no tienen default: si falta uno, el pipeline falla al arrancar en vez de
conectarse a un destino equivocado.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from contract import load_contract

REPO_ROOT = Path(__file__).resolve().parent

load_dotenv(REPO_ROOT / ".env")


# ---------------------------------------------------------------------------
# Layout del extracto FBL1N
# ---------------------------------------------------------------------------
# El layout sale de contracts/fbl1n_extract.yml, que es tambien lo que valida
# el schema check. Aca se exponen las mismas columnas con los nombres que ya
# usaban los scripts, para que el contrato sea uno solo.
#
# (encabezado literal en el xlsx, campo SAP / nombre de columna en RAW)
CONTRACT = load_contract()
COLUMNS: tuple[tuple[str, str], ...] = tuple((c.header, c.field) for c in CONTRACT.columns)

EXCEL_HEADERS: tuple[str, ...] = CONTRACT.headers
RAW_COLUMNS: tuple[str, ...] = CONTRACT.fields

# Indices posicionales en el orden del contrato: es el orden en que el
# generador escribe el xlsx y normalize.py el CSV que levanta COPY INTO.
COL_INDEX: dict[str, int] = {c: i for i, c in enumerate(RAW_COLUMNS)}

# Clave del documento contable en SAP. Los extractos anteriores a la version 2
# del contrato no la traen completa: el generador y staging la derivan con
# estos valores (sociedad unica, ejercicio = ano de BUDAT, posicion 1).
DOCUMENT_KEY: tuple[str, ...] = ("BUKRS", "BELNR", "GJAHR", "BUZEI")
DEFAULT_COMPANY_CODE = "1000"
DEFAULT_LINE_ITEM = 1

DATE_COLUMNS: tuple[str, ...] = ("BLDAT", "BUDAT", "NETDT", "AUGDT")
AMOUNT_COLUMNS: tuple[str, ...] = ("WRBTR", "DMBTR")

# Columnas que la variante trae siempre sin poblar.
ALWAYS_EMPTY_COLUMNS: tuple[str, ...] = ("ICON_STATUS", "XREF1", "ZLSCH", "ZLSPR")

# Fila de totales que SAP agrega al pie del report.
TOTAL_ROW_MARKER = r"@5C\QPendientes@"
# SAP no suma importes en moneda de documento cuando el report mezcla monedas.
TOTAL_ROW_DOC_AMOUNT = "*"
# Tolerancia del checksum contra la suma de las lineas de detalle, en ARS.
CHECKSUM_TOLERANCE = 0.01

# Formato de fecha del display de SAP. El CSV que aterriza en RAW conserva el
# texto tal como lo muestra el report; el casteo ocurre recien en staging.
SAP_DATE_FORMAT = "%d.%m.%Y"

EXCEL_SHEET_NAME = CONTRACT.sheet
EXTRACT_FILENAME_TEMPLATE = "FBL1N_{yyyymmdd}.xlsx"
EXTRACT_FILENAME_GLOB = "FBL1N_*.xlsx"
STAGING_FILENAME_TEMPLATE = "FBL1N_{yyyymmdd}.csv"
STAGING_FILENAME_GLOB = "FBL1N_*.csv"

# Cuenta de mayor de contrapartida del ledger de acreedores.
RECON_ACCOUNT = "9975101"
LOCAL_CURRENCY = "ARS"


# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
LANDING_DIR = Path(os.environ.get("LANDING_DIR") or REPO_ROOT / "landing")
STAGING_DIR = Path(os.environ.get("STAGING_DIR") or REPO_ROOT / "staging_files")
WEB_DATA_DIR = Path(os.environ.get("WEB_DATA_DIR") or REPO_ROOT / "web" / "public" / "data")
SEEDS_DIR = REPO_ROOT / "dbt_ap" / "seeds"
STATE_PATH = LANDING_DIR / "_state.json"
FX_RATES_PATH = SEEDS_DIR / "fx_rates.csv"
VENDORS_PATH = SEEDS_DIR / "vendors.csv"
DOCUMENT_TYPES_PATH = SEEDS_DIR / "document_types.csv"


# ---------------------------------------------------------------------------
# Entorno
# ---------------------------------------------------------------------------
class ConfigError(RuntimeError):
    """Falta configuracion obligatoria o es inconsistente."""


def require_env(name: str) -> str:
    """Devuelve una variable de entorno obligatoria.

    Sin default a proposito: un default silencioso sobre una credencial o un
    nombre de objeto es como termina un pipeline escribiendo en la base
    equivocada.
    """
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(
            f"Falta la variable de entorno {name}. "
            "Copiar .env.example a .env y completarla, o exportarla en el runner."
        )
    return value


def optional_env(name: str, default: str = "") -> str:
    return os.environ.get(name, "").strip() or default


def snowflake_objects() -> dict[str, str]:
    """Nombres de los objetos Snowflake que toca el pipeline."""
    return {
        "database": require_env("SNOWFLAKE_DATABASE"),
        "schema_raw": require_env("SNOWFLAKE_SCHEMA_RAW"),
        "schema_analytics": require_env("SNOWFLAKE_SCHEMA_ANALYTICS"),
        "stage": require_env("SNOWFLAKE_STAGE"),
        "file_format": require_env("SNOWFLAKE_FILE_FORMAT"),
        "raw_table": require_env("SNOWFLAKE_RAW_TABLE"),
    }
