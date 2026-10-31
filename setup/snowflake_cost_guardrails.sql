-- =============================================================================
-- setup/snowflake_cost_guardrails.sql -- tope de costo y acceso minimo
-- =============================================================================
-- Lo corre una persona, a mano, como ACCOUNTADMIN, desde una worksheet o
-- SnowSQL. Ningun workflow ni script del repo lo ejecuta.
--
-- Idempotente: se puede volver a correr. Fija los valores aunque los objetos ya
-- existan (sql/00_setup_account.sql los crea con una version anterior de estos
-- mismos limites).
--
-- Requiere haber corrido antes sql/00_setup_account.sql, que crea la base, los
-- schemas y el usuario de servicio AP_PIPELINE_SVC con su clave publica.
--
-- NOTIFY_USERS (seccion 2) es el usuario humano que recibe los avisos del
-- resource monitor: tiene que tener el email verificado en Snowflake. Un
-- usuario TYPE = SERVICE no recibe mails.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 1. Warehouse: el mas chico posible, apagado apenas termina
-- -----------------------------------------------------------------------------
-- AUTO_SUSPEND = 60 es el minimo que respeta la facturacion: cada resume cobra
-- 60 segundos completos, asi que suspender antes no ahorra nada.
CREATE WAREHOUSE IF NOT EXISTS AP_WH
    WAREHOUSE_SIZE      = 'XSMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Pipeline de cuentas a pagar: ingesta, dbt y export.';

ALTER WAREHOUSE AP_WH SET
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    -- Una consulta que se cuelga no consume la cuota del mes: se corta a los
    -- 30 minutos. La corrida diaria completa tarda menos de 3.
    STATEMENT_TIMEOUT_IN_SECONDS = 1800;

-- -----------------------------------------------------------------------------
-- 2. Resource monitor: 10 creditos por mes, avisos al 50/80 % y corte al 100 %
-- -----------------------------------------------------------------------------
-- FREQUENCY y START_TIMESTAMP solo se fijan al crearlo: un ALTER posterior
-- reiniciaria el contador del periodo.
CREATE RESOURCE MONITOR IF NOT EXISTS AP_RM
    WITH CREDIT_QUOTA = 10
    FREQUENCY         = MONTHLY
    START_TIMESTAMP   = IMMEDIATELY;

ALTER RESOURCE MONITOR AP_RM SET
    CREDIT_QUOTA = 10
    NOTIFY_USERS = (joelsuarez3470)
    TRIGGERS
        ON 50  PERCENT DO NOTIFY
        ON 80  PERCENT DO NOTIFY
        -- SUSPEND deja terminar lo que esta corriendo y no acepta mas. Con
        -- SUSPEND_IMMEDIATE se cortaria una carga a mitad de transaccion.
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE AP_WH SET RESOURCE_MONITOR = AP_RM;

-- -----------------------------------------------------------------------------
-- 3. Rol de servicio con los privilegios minimos
-- -----------------------------------------------------------------------------
-- El pipeline crea y es dueno de sus objetos (la tabla RAW, el stage, las
-- tablas y vistas de dbt), y el dueno conserva todos los privilegios sobre lo
-- suyo. A nivel schema alcanza con USAGE y con poder crear el tipo de objeto
-- que va en cada uno. Sin DROP SCHEMA, sin acceso a otras bases.
CREATE ROLE IF NOT EXISTS AP_PIPELINE
    COMMENT = 'Rol de servicio del pipeline: ingesta, dbt y export.';
GRANT ROLE AP_PIPELINE TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE AP_WH TO ROLE AP_PIPELINE;
GRANT USAGE ON DATABASE AP_ANALYTICS TO ROLE AP_PIPELINE;
-- dbt crea el schema TEST_FAILURES la primera vez que guarda fallas.
GRANT CREATE SCHEMA ON DATABASE AP_ANALYTICS TO ROLE AP_PIPELINE;

-- sql/00_setup_account.sql otorgaba ALL PRIVILEGES: se reemplaza por lo justo.
REVOKE ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.RAW          FROM ROLE AP_PIPELINE;
REVOKE ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.ANALYTICS    FROM ROLE AP_PIPELINE;
REVOKE ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.STAGING      FROM ROLE AP_PIPELINE;
REVOKE ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.INTERMEDIATE FROM ROLE AP_PIPELINE;
REVOKE ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.MARTS        FROM ROLE AP_PIPELINE;
REVOKE ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.SNAPSHOTS    FROM ROLE AP_PIPELINE;
REVOKE ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.SEEDS        FROM ROLE AP_PIPELINE;

-- RAW: la tabla del change feed, su stage interno y el file format del CSV.
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
    ON SCHEMA AP_ANALYTICS.RAW TO ROLE AP_PIPELINE;

-- Capas de dbt: vistas (staging, intermediate) y tablas (marts, seeds, snapshots).
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA AP_ANALYTICS.ANALYTICS    TO ROLE AP_PIPELINE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA AP_ANALYTICS.STAGING      TO ROLE AP_PIPELINE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA AP_ANALYTICS.INTERMEDIATE TO ROLE AP_PIPELINE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA AP_ANALYTICS.MARTS        TO ROLE AP_PIPELINE;
GRANT USAGE, CREATE TABLE              ON SCHEMA AP_ANALYTICS.SNAPSHOTS    TO ROLE AP_PIPELINE;
GRANT USAGE, CREATE TABLE              ON SCHEMA AP_ANALYTICS.SEEDS        TO ROLE AP_PIPELINE;

-- -----------------------------------------------------------------------------
-- 4. Usuario de servicio: solo key-pair, sin password
-- -----------------------------------------------------------------------------
-- El usuario lo crea sql/00_setup_account.sql con su clave publica; aca solo
-- se fijan sus defaults. TYPE = SERVICE no admite password ni login
-- interactivo: la unica forma de autenticarse es la clave privada.
ALTER USER AP_PIPELINE_SVC SET
    TYPE              = SERVICE
    DEFAULT_ROLE      = AP_PIPELINE
    DEFAULT_WAREHOUSE = AP_WH;

GRANT ROLE AP_PIPELINE TO USER AP_PIPELINE_SVC;

-- Rotacion de clave sin corte: cargar la nueva en RSA_PUBLIC_KEY_2, actualizar
-- el secret y el .env, y recien despues borrar la anterior.
--   ALTER USER AP_PIPELINE_SVC SET RSA_PUBLIC_KEY_2 = '<<NUEVA_RSA_PUBLIC_KEY>>';
--   ALTER USER AP_PIPELINE_SVC UNSET RSA_PUBLIC_KEY;

-- -----------------------------------------------------------------------------
-- 5. Verificacion
-- -----------------------------------------------------------------------------
SHOW WAREHOUSES LIKE 'AP_WH';
SHOW RESOURCE MONITORS LIKE 'AP_RM';
SHOW GRANTS TO ROLE AP_PIPELINE;
DESC USER AP_PIPELINE_SVC;
