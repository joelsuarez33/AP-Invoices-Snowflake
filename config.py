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

REPO_ROOT = Path(__file__).resolve().parent

load_dotenv(REPO_ROOT / ".env")


# ---------------------------------------------------------------------------
# Layout del extracto FBL1N
# ---------------------------------------------------------------------------
# La variante de layout usada en SAP exporta 21 columnas con encabezados en
# espanol. El orden es parte del contrato: normalize.py valida la secuencia
# completa antes de tocar una sola fila.
#
# (encabezado literal en el xlsx, campo SAP / nombre de columna en RAW)
COLUMNS: tuple[tuple[str, str], ...] = (
    ("Icono part.abiertas/comp.", "ICON_STATUS"),
    ("Acreedor", "LIFNR"),
    ("Clave de referencia", "KIDNO"),
    ("Referencia", "XBLNR"),
    ("Nº documento", "BELNR"),
    ("Clave referencia 1", "XREF1"),
    ("Vía de pago", "ZLSCH"),
    ("Bloqueo de pago", "ZLSPR"),
    ("Demora tras vencimiento neto", "VERZN"),
    ("Importe en moneda doc.", "WRBTR"),
    ("Moneda del documento", "WAERS"),
    ("Importe en moneda local", "DMBTR"),
    ("Moneda local", "HWAER"),
    ("Fecha de documento", "BLDAT"),
    ("Fe.contabilización", "BUDAT"),
    ("Vencimiento neto", "NETDT"),
    ("Fecha compensación", "AUGDT"),
    ("Doc.compensación", "AUGBL"),
    ("Clase de documento", "BLART"),
    ("Nombre del usuario", "USNAM"),
    ("Cta.contrapartida", "GKONT"),
)

EXCEL_HEADERS: tuple[str, ...] = tuple(h for h, _ in COLUMNS)
RAW_COLUMNS: tuple[str, ...] = tuple(c for _, c in COLUMNS)

# Indices posicionales usados por generate_extract.py y normalize.py.
COL_INDEX: dict[str, int] = {c: i for i, c in enumerate(RAW_COLUMNS)}

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

EXCEL_SHEET_NAME = "Sheet1"
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
