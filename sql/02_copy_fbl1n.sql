-- =============================================================================
-- 02_copy_fbl1n.sql -- ingesta del change feed a RAW
-- =============================================================================
-- Es el mismo SQL que emite load.py (ver load.py::copy_statement). Se mantiene
-- versionado aparte para poder correrlo a mano desde una worksheet cuando hay
-- que diagnosticar una carga, y para que la sentencia sea revisable en el PR
-- sin leer Python.
--
-- Idempotencia: sin FORCE, COPY INTO consulta el load history del stage y saltea
-- los archivos ya cargados durante 64 dias. No hay deduplicacion propia en la
-- ingesta a proposito -- seria reimplementar peor una garantia del motor.
-- =============================================================================

USE ROLE AP_PIPELINE;
USE WAREHOUSE AP_WH;
USE DATABASE AP_ANALYTICS;
USE SCHEMA RAW;

-- -----------------------------------------------------------------------------
-- 1. Subir los CSV normalizados (equivalente a lo que hace load.py con PUT)
-- -----------------------------------------------------------------------------
-- PUT no se puede ejecutar desde una worksheet web: correrlo con SnowSQL o
-- dejarlo en manos de load.py.
--   PUT 'file:///ruta/al/repo/staging_files/FBL1N_20260907.csv' @STG_FBL1N
--       AUTO_COMPRESS = TRUE OVERWRITE = FALSE PARALLEL = 4;

LIST @STG_FBL1N PATTERN = '.*FBL1N_[0-9]{8}[.]csv([.]gz)?';

-- -----------------------------------------------------------------------------
-- 2. COPY INTO con transformacion
-- -----------------------------------------------------------------------------
-- La metadata de carga se agrega en el SELECT: METADATA$FILENAME y
-- METADATA$FILE_ROW_NUMBER identifican el origen exacto de cada fila, y
-- _EXTRACT_DATE se parsea del nombre del archivo -- no de la fecha de carga,
-- que cambiaria en un re-proceso y arruinaria el orden del change feed.
COPY INTO AP_ANALYTICS.RAW.FBL1N_ITEMS (
        ICON_STATUS, LIFNR, KIDNO, XBLNR, BELNR, XREF1, ZLSCH, ZLSPR, VERZN,
        WRBTR, WAERS, DMBTR, HWAER, BLDAT, BUDAT, NETDT, AUGDT, AUGBL,
        BLART, USNAM, GKONT,
        _SOURCE_FILE, _FILE_ROW_NUMBER, _LOADED_AT, _EXTRACT_DATE
)
FROM (
    SELECT
        t.$1,  t.$2,  t.$3,  t.$4,  t.$5,  t.$6,  t.$7,  t.$8,  t.$9,
        t.$10, t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18,
        t.$19, t.$20, t.$21,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP(),
        TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'FBL1N_([0-9]{8})', 1, 1, 'e', 1), 'YYYYMMDD')
    FROM @STG_FBL1N t
)
PATTERN     = '.*FBL1N_[0-9]{8}[.]csv([.]gz)?'
FILE_FORMAT = (FORMAT_NAME = 'FF_FBL1N_CSV')
ON_ERROR    = 'ABORT_STATEMENT';

-- -----------------------------------------------------------------------------
-- 3. Verificacion de la carga
-- -----------------------------------------------------------------------------
SELECT
    _EXTRACT_DATE,
    COUNT(*)                     AS filas,
    COUNT(DISTINCT BELNR)        AS documentos,
    COUNT_IF(AUGDT IS NOT NULL)  AS compensaciones,
    MIN(_LOADED_AT)              AS cargado_desde,
    MAX(_LOADED_AT)              AS cargado_hasta
FROM AP_ANALYTICS.RAW.FBL1N_ITEMS
GROUP BY _EXTRACT_DATE
ORDER BY _EXTRACT_DATE DESC
LIMIT 20;

-- Documentos con mas de una version en RAW: correcciones y compensaciones.
-- Esta consulta es el argumento de por que RAW es append-only.
SELECT
    BELNR,
    COUNT(*)                  AS versiones,
    MIN(_EXTRACT_DATE)        AS primera_aparicion,
    MAX(_EXTRACT_DATE)        AS ultima_aparicion
FROM AP_ANALYTICS.RAW.FBL1N_ITEMS
GROUP BY BELNR
HAVING COUNT(*) > 1
ORDER BY versiones DESC, BELNR
LIMIT 20;

-- Ultimas cargas registradas en el load history del stage.
SELECT
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    LAST_LOAD_TIME
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME  => 'AP_ANALYTICS.RAW.FBL1N_ITEMS',
        START_TIME  => DATEADD('day', -14, CURRENT_TIMESTAMP())
    )
)
ORDER BY LAST_LOAD_TIME DESC;
