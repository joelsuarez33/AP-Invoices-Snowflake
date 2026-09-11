-- =============================================================================
-- 99_teardown.sql -- desmonta todo lo que crea el proyecto
-- =============================================================================
-- Destructivo. Borra la base entera, incluido el historial de RAW, que no se
-- puede reconstruir desde Snowflake: solo replicando landing/ con normalize.py
-- y load.py.
--
-- Para usarlo hay que descomentar los DROP. Vienen comentados a proposito: un
-- teardown que se ejecuta solo con pegarlo en una worksheet es un accidente
-- esperando a pasar.
--
-- Antes de correrlo conviene mirar que se va a perder:
--   SELECT COUNT(*), MIN(_EXTRACT_DATE), MAX(_EXTRACT_DATE)
--   FROM AP_ANALYTICS.RAW.FBL1N_ITEMS;
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- Inventario previo
-- -----------------------------------------------------------------------------
SHOW SCHEMAS IN DATABASE AP_ANALYTICS;
SHOW TABLES IN SCHEMA AP_ANALYTICS.RAW;

-- -----------------------------------------------------------------------------
-- Objetos de datos
-- -----------------------------------------------------------------------------
-- La base queda en Time Travel el tiempo que fije DATA_RETENTION_TIME_IN_DAYS;
-- UNDROP DATABASE AP_ANALYTICS la recupera dentro de esa ventana.
-- DROP DATABASE IF EXISTS AP_ANALYTICS;

-- -----------------------------------------------------------------------------
-- Computo
-- -----------------------------------------------------------------------------
-- DROP WAREHOUSE IF EXISTS AP_WH;

-- -----------------------------------------------------------------------------
-- Identidades
-- -----------------------------------------------------------------------------
-- Borrar el usuario invalida el key-pair: la clave privada que quede en .env o
-- en los secrets de GitHub deja de servir y hay que regenerar el par entero.
-- DROP USER IF EXISTS AP_PIPELINE_SVC;
-- DROP ROLE IF EXISTS AP_READER;
-- DROP ROLE IF EXISTS AP_PIPELINE;
