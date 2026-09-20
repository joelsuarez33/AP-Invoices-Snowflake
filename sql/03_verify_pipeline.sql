-- =============================================================================
-- 03_verify_pipeline.sql -- verificacion manual del estado del pipeline
-- =============================================================================
-- Se corre a mano, despues de un dbt build, cuando hay que confirmar que el
-- ledger esta como se espera. Solo lee: ningun statement modifica nada.
--
-- Los tests de dbt ya cubren estas invariantes en cada corrida. Esto es para
-- mirar el estado con los ojos, que es otra cosa: cuanto historial hay, cuantos
-- documentos volvieron a aparecer, y si la mart cuadra con RAW.
-- =============================================================================

USE ROLE AP_PIPELINE;
USE WAREHOUSE AP_WH;
USE DATABASE AP_ANALYTICS;

-- -----------------------------------------------------------------------------
-- 1. Volumen y cobertura de RAW
-- -----------------------------------------------------------------------------
-- Un extracto por dia habil. FILAS > DOCUMENTOS es lo normal: RAW es
-- append-only y guarda una fila por cada re-emision.
SELECT
    COUNT(*)                       AS filas,
    COUNT(DISTINCT BELNR)          AS documentos,
    COUNT(DISTINCT _EXTRACT_DATE)  AS dias,
    MIN(_EXTRACT_DATE)             AS primer_extracto,
    MAX(_EXTRACT_DATE)             AS fecha_de_corte
FROM RAW.FBL1N_ITEMS;

-- Filas por extracto. Un feed diario trae entre 40 y 80 filas. Miles de filas
-- significan que el generador reconstruyo el ledger en vez de continuarlo.
SELECT
    _EXTRACT_DATE,
    COUNT(*) AS filas
FROM RAW.FBL1N_ITEMS
GROUP BY _EXTRACT_DATE
ORDER BY _EXTRACT_DATE DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 2. El change feed esta funcionando
-- -----------------------------------------------------------------------------
-- Documentos con mas de una version: compensaciones y correcciones de importe.
-- Si diera cero, el extracto seria un feed de altas y el QUALIFY de staging no
-- tendria nada que resolver.
SELECT COUNT(*) AS documentos_con_varias_versiones
FROM (
    SELECT BELNR
    FROM RAW.FBL1N_ITEMS
    GROUP BY BELNR
    HAVING COUNT(*) > 1
);

-- Un archivo cargado dos veces se ve como filas repetidas con el mismo
-- numero de linea. Tiene que devolver cero: la idempotencia la da el load
-- history del stage.
SELECT
    _SOURCE_FILE,
    COUNT(*)                            AS filas,
    COUNT(DISTINCT _FILE_ROW_NUMBER)    AS lineas_distintas
FROM RAW.FBL1N_ITEMS
GROUP BY _SOURCE_FILE
HAVING COUNT(*) > COUNT(DISTINCT _FILE_ROW_NUMBER);

-- -----------------------------------------------------------------------------
-- 3. La mart cuadra con RAW
-- -----------------------------------------------------------------------------
-- Es el mismo control que hace assert_mart_reconciles_with_raw, reescrito para
-- ver los numeros en vez de un pass/fail. Reaplica el QUALIFY de staging sobre
-- RAW: si los inner join de la capa intermedia descartaron filas, el delta lo
-- muestra.
WITH RAW_DEDUPLICADO AS (
    SELECT CAST(NULLIF(TRIM(DMBTR), '') AS NUMBER(19, 2)) AS DMBTR
    FROM RAW.FBL1N_ITEMS
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY BELNR
        ORDER BY _EXTRACT_DATE DESC, _FILE_ROW_NUMBER DESC
    ) = 1
)

SELECT
    (SELECT COUNT(*) FROM RAW_DEDUPLICADO)              AS partidas_raw,
    (SELECT COUNT(*) FROM MARTS.FCT_AP_OPEN_ITEMS)      AS partidas_mart,
    (SELECT COUNT(*) FROM RAW_DEDUPLICADO)
        - (SELECT COUNT(*) FROM MARTS.FCT_AP_OPEN_ITEMS) AS delta_partidas,
    ROUND(
        (SELECT SUM(DMBTR) FROM RAW_DEDUPLICADO)
        - (SELECT SUM(DMBTR) FROM MARTS.FCT_AP_OPEN_ITEMS), 2
    )                                                    AS delta_importe_ars;

-- -----------------------------------------------------------------------------
-- 4. Cobertura de la serie de cotizaciones
-- -----------------------------------------------------------------------------
-- El seed de FX tiene que cubrir el rango completo de BLDAT. Cuando no lo
-- cubrio, 754 documentos llegaron a la capa intermedia sin cotizacion.
SELECT
    (SELECT MIN(TO_DATE(BLDAT, 'DD.MM.YYYY')) FROM RAW.FBL1N_ITEMS) AS primer_bldat,
    (SELECT MAX(TO_DATE(BLDAT, 'DD.MM.YYYY')) FROM RAW.FBL1N_ITEMS) AS ultimo_bldat,
    (SELECT MIN(RATE_DATE) FROM SEEDS.FX_RATES)                     AS fx_desde,
    (SELECT MAX(RATE_DATE) FROM SEEDS.FX_RATES)                     AS fx_hasta,
    (SELECT COUNT(*) FROM MARTS.FCT_AP_OPEN_ITEMS
     WHERE FX_RATE_AT_DOCUMENT_DATE IS NULL)                        AS sin_cotizacion;

-- -----------------------------------------------------------------------------
-- 5. Historia del snapshot
-- -----------------------------------------------------------------------------
-- VERSIONES_CERRADAS cuenta las correcciones de importe que el snapshot
-- registro. Un backfill cargado en un solo dbt build no siembra historia: la
-- acumula desde las corridas diarias.
SELECT
    COUNT(*)                                    AS filas,
    COUNT(DISTINCT BELNR)                       AS documentos,
    COUNT_IF(DBT_VALID_TO IS NOT NULL)          AS versiones_cerradas
FROM SNAPSHOTS.SNAP_AP_AMOUNTS;

-- -----------------------------------------------------------------------------
-- 6. Fallas de tests almacenadas
-- -----------------------------------------------------------------------------
-- Una tabla por test con store_failures: los 10 relationships y los 4 tests
-- singulares. Para vaciar el esquema: scripts/cleanup_test_failures.py --drop
SELECT
    TABLE_NAME,
    ROW_COUNT,
    LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'TEST_FAILURES'
    AND ROW_COUNT > 0
ORDER BY LAST_ALTERED DESC;
