-- =============================================================================
-- 01_setup_raw.sql -- zona de aterrizaje append-only del extracto FBL1N
-- =============================================================================
-- Se corre una vez, con AP_PIPELINE, despues de 00_setup_account.sql.
--
-- Dos decisiones de diseno que se sostienen en toda la capa:
--
--   1. Todas las columnas de negocio son STRING. RAW guarda una imagen fiel del
--      texto que emitio SAP. Castear en la ingesta significa que un dato con
--      formato inesperado hace fallar la carga en vez de quedar registrado y
--      visible; el casteo vive en staging, donde se puede testear.
--
--   2. Nunca se borra ni se sobrescribe. Un mismo BELNR acumula una fila por
--      cada vez que el change feed lo re-emitio. Ese historial reemplaza a los
--      snapshots completos: la version actual se resuelve con un QUALIFY en
--      staging, y las versiones previas quedan disponibles para auditoria.
-- =============================================================================

USE ROLE AP_PIPELINE;
USE WAREHOUSE AP_WH;
USE DATABASE AP_ANALYTICS;
USE SCHEMA RAW;

-- -----------------------------------------------------------------------------
-- File format
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT FF_FBL1N_CSV
    TYPE                         = CSV
    FIELD_DELIMITER              = ','
    RECORD_DELIMITER             = '\n'
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE                   = FALSE
    -- normalize.py emite cadena vacia para toda columna sin valor.
    EMPTY_FIELD_AS_NULL          = TRUE
    NULL_IF                      = ('')
    ENCODING                     = 'UTF8'
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    COMPRESSION                  = AUTO
    COMMENT = 'CSV normalizado de FBL1N. 21 columnas, todo texto.';

-- -----------------------------------------------------------------------------
-- Stage interno
-- -----------------------------------------------------------------------------
-- Interno y no externo: el proyecto no depende de una cuenta de object storage
-- adicional, y el load history del stage es justamente lo que da idempotencia
-- a la ingesta.
CREATE STAGE IF NOT EXISTS STG_FBL1N
    FILE_FORMAT = FF_FBL1N_CSV
    DIRECTORY   = (ENABLE = TRUE)
    COMMENT = 'Aterrizaje de los CSV normalizados del change feed diario.';

-- -----------------------------------------------------------------------------
-- Tabla RAW
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FBL1N_ITEMS (
    -- --- columnas de negocio: texto crudo, tal como lo emitio el report ---
    ICON_STATUS       STRING COMMENT 'Icono part.abiertas/comp. -- presentacion, siempre vacio en esta variante',
    LIFNR             STRING COMMENT 'Acreedor',
    KIDNO             STRING COMMENT 'Clave de referencia de pago',
    XBLNR             STRING COMMENT 'Referencia del proveedor',
    BELNR             STRING COMMENT 'Nro de documento -- clave del merge',
    XREF1             STRING COMMENT 'Clave referencia 1 -- sin poblar en esta variante',
    ZLSCH             STRING COMMENT 'Via de pago -- sin poblar en esta variante',
    ZLSPR             STRING COMMENT 'Bloqueo de pago -- sin poblar en esta variante',
    VERZN             STRING COMMENT 'Demora tras vencimiento neto -- calculo del report, no un hecho del documento',
    WRBTR             STRING COMMENT 'Importe en moneda de documento -- NO sumable entre monedas',
    WAERS             STRING COMMENT 'Moneda del documento',
    DMBTR             STRING COMMENT 'Importe en moneda local -- importe de referencia',
    HWAER             STRING COMMENT 'Moneda local',
    BLDAT             STRING COMMENT 'Fecha de documento (DD.MM.YYYY)',
    BUDAT             STRING COMMENT 'Fecha de contabilizacion (DD.MM.YYYY)',
    NETDT             STRING COMMENT 'Vencimiento neto (DD.MM.YYYY)',
    AUGDT             STRING COMMENT 'Fecha de compensacion (DD.MM.YYYY). NULL = partida abierta',
    AUGBL             STRING COMMENT 'Documento de compensacion',
    BLART             STRING COMMENT 'Clase de documento',
    USNAM             STRING COMMENT 'Usuario que contabilizo',
    GKONT             STRING COMMENT 'Cuenta de contrapartida',

    -- --- metadata de carga ---
    _SOURCE_FILE      STRING       NOT NULL COMMENT 'METADATA$FILENAME del stage',
    _FILE_ROW_NUMBER  NUMBER(38,0) NOT NULL COMMENT 'METADATA$FILE_ROW_NUMBER -- desempata dentro de un archivo',
    _LOADED_AT        TIMESTAMP_NTZ NOT NULL COMMENT 'Momento del COPY INTO',
    _EXTRACT_DATE     DATE         NOT NULL COMMENT 'Fecha del extracto, parseada del nombre del archivo'
)
COMMENT = 'Append-only. Un BELNR puede tener varias filas: una por cada re-emision.';

-- Ordenar el micro-particionado por fecha de extracto: todos los filtros
-- incrementales aguas abajo son por _EXTRACT_DATE.


-- -----------------------------------------------------------------------------
-- Verificacion
-- -----------------------------------------------------------------------------
SHOW STAGES LIKE 'STG_FBL1N';
DESCRIBE TABLE FBL1N_ITEMS;
