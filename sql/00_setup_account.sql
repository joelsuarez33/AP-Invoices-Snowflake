-- =============================================================================
-- 00_setup_account.sql -- objetos de cuenta del pipeline de cuentas a pagar
-- =============================================================================
-- Se corre UNA vez, con ACCOUNTADMIN, antes de la primera carga.
--
-- No contiene ninguna credencial. El ALTER USER que registra la clave publica
-- RSA queda comentado al final a proposito: se ejecuta a mano, con la clave
-- real, y nunca se versiona. El par de claves vive entero fuera del repo.
--
-- Generacion del par (Git Bash):
--   mkdir -p ~/.snowflake && cd ~/.snowflake
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--   grep -v "^-----" rsa_key.pub | tr -d '\n' | clip
--
-- Los nombres de abajo tienen que coincidir con lo que se cargue en .env.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- Warehouse
-- -----------------------------------------------------------------------------
-- AUTO_SUSPEND al minimo permitido: cada resume factura 60 segundos completos,
-- asi que lo que importa no es suspender rapido sino agrupar todo el trabajo
-- del dia en una sola conexion. El pipeline lo hace.
CREATE WAREHOUSE IF NOT EXISTS AP_WH
    WAREHOUSE_SIZE       = 'XSMALL'
    AUTO_SUSPEND         = 60
    AUTO_RESUME          = TRUE
    INITIALLY_SUSPENDED  = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 1800
    COMMENT = 'Ingesta y transformacion del ledger de acreedores';

-- -----------------------------------------------------------------------------
-- Techo de gasto
-- -----------------------------------------------------------------------------
-- El riesgo real de costo no es la frecuencia del cron sino un warehouse que
-- queda corriendo. 10 creditos/mes son ~5x el consumo esperado del pipeline.
CREATE RESOURCE MONITOR IF NOT EXISTS AP_RM
    WITH CREDIT_QUOTA     = 10
         FREQUENCY        = MONTHLY
         START_TIMESTAMP  = IMMEDIATELY
    TRIGGERS ON  80 PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND
             ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE AP_WH SET RESOURCE_MONITOR = AP_RM;

-- -----------------------------------------------------------------------------
-- Base y esquemas
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS AP_ANALYTICS
    COMMENT = 'Cuentas a pagar -- extracto FBL1N';

-- RAW es append-only: la zona de aterrizaje del change feed, sin transformar.
CREATE SCHEMA IF NOT EXISTS AP_ANALYTICS.RAW
    COMMENT = 'Aterrizaje append-only del extracto FBL1N. Todo STRING.';

-- Un esquema por capa dbt. El macro generate_schema_name los usa literalmente.
CREATE SCHEMA IF NOT EXISTS AP_ANALYTICS.ANALYTICS    COMMENT = 'Esquema por defecto de dbt';
CREATE SCHEMA IF NOT EXISTS AP_ANALYTICS.STAGING      COMMENT = 'Capa staging (views)';
CREATE SCHEMA IF NOT EXISTS AP_ANALYTICS.INTERMEDIATE COMMENT = 'Capa intermedia';
CREATE SCHEMA IF NOT EXISTS AP_ANALYTICS.MARTS        COMMENT = 'Marts de consumo';
CREATE SCHEMA IF NOT EXISTS AP_ANALYTICS.SNAPSHOTS    COMMENT = 'SCD2 de importes';
CREATE SCHEMA IF NOT EXISTS AP_ANALYTICS.SEEDS        COMMENT = 'Maestros versionados en el repo';

-- -----------------------------------------------------------------------------
-- Roles
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS AP_PIPELINE
    COMMENT = 'Ingesta + dbt. Escribe en todos los esquemas de AP_ANALYTICS.';
CREATE ROLE IF NOT EXISTS AP_READER
    COMMENT = 'Solo lectura sobre MARTS. Consumo del dashboard.';

GRANT ROLE AP_PIPELINE TO ROLE SYSADMIN;
GRANT ROLE AP_READER   TO ROLE AP_PIPELINE;

GRANT USAGE ON WAREHOUSE AP_WH TO ROLE AP_PIPELINE;
GRANT USAGE ON WAREHOUSE AP_WH TO ROLE AP_READER;

GRANT USAGE ON DATABASE AP_ANALYTICS TO ROLE AP_PIPELINE;
GRANT USAGE ON DATABASE AP_ANALYTICS TO ROLE AP_READER;

-- dbt crea el esquema dbt_test__audit por su cuenta cuando un test tiene
-- store_failures activo. Sin este grant, el primer dbt build con tests falla.
GRANT CREATE SCHEMA ON DATABASE AP_ANALYTICS TO ROLE AP_PIPELINE;

GRANT ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.RAW          TO ROLE AP_PIPELINE;
GRANT ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.ANALYTICS    TO ROLE AP_PIPELINE;
GRANT ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.STAGING      TO ROLE AP_PIPELINE;
GRANT ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.INTERMEDIATE TO ROLE AP_PIPELINE;
GRANT ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.MARTS        TO ROLE AP_PIPELINE;
GRANT ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.SNAPSHOTS    TO ROLE AP_PIPELINE;
GRANT ALL PRIVILEGES ON SCHEMA AP_ANALYTICS.SEEDS        TO ROLE AP_PIPELINE;

GRANT USAGE ON SCHEMA AP_ANALYTICS.MARTS TO ROLE AP_READER;
GRANT SELECT ON ALL TABLES    IN SCHEMA AP_ANALYTICS.MARTS TO ROLE AP_READER;
GRANT SELECT ON ALL VIEWS     IN SCHEMA AP_ANALYTICS.MARTS TO ROLE AP_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA AP_ANALYTICS.MARTS TO ROLE AP_READER;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA AP_ANALYTICS.MARTS TO ROLE AP_READER;

-- -----------------------------------------------------------------------------
-- Usuario de servicio -- autenticacion por key-pair, sin password
-- -----------------------------------------------------------------------------
-- TYPE = SERVICE no admite password: no acepta MUST_CHANGE_PASSWORD ni login
-- por la UI. Solo acceso programatico con key-pair.
CREATE USER IF NOT EXISTS AP_PIPELINE_SVC
    LOGIN_NAME        = 'AP_PIPELINE_SVC'
    DISPLAY_NAME      = 'AP pipeline service account'
    DEFAULT_ROLE      = AP_PIPELINE
    DEFAULT_WAREHOUSE = AP_WH
    DEFAULT_NAMESPACE = AP_ANALYTICS.RAW
    TYPE              = SERVICE
    COMMENT = 'Cuenta de servicio del pipeline. Sin password: solo key-pair.';

GRANT ROLE AP_PIPELINE TO USER AP_PIPELINE_SVC;

-- -----------------------------------------------------------------------------
-- Verificacion
-- -----------------------------------------------------------------------------
SHOW ROLES LIKE 'AP_%';
SHOW GRANTS TO ROLE AP_PIPELINE;
SHOW GRANTS TO USER AP_PIPELINE_SVC;
SHOW PARAMETERS LIKE 'RESOURCE_MONITOR' IN WAREHOUSE AP_WH;
DESCRIBE USER AP_PIPELINE_SVC;

-- =============================================================================
-- PASO MANUAL -- no forma parte del Run All
-- =============================================================================
-- Ejecutar aparte, reemplazando la cadena por la clave publica real en UNA sola
-- linea, sin las lineas BEGIN/END. Debe arrancar con MIIBIjANBgkqhkiG9w0BAQEF
-- y medir ~392 caracteres. Si arranca con MIIE es la clave PRIVADA: no pegarla.
--
--   ALTER USER AP_PIPELINE_SVC SET RSA_PUBLIC_KEY = 'MIIBIjANBgkq...';
--
-- Verificar despues que RSA_PUBLIC_KEY_FP quede poblado con un SHA256:
--
--   DESCRIBE USER AP_PIPELINE_SVC;
--
-- Rotacion: se publica la nueva en el segundo slot, se despliega, y recien
-- despues se desasigna la vieja.
--
--   ALTER USER AP_PIPELINE_SVC SET RSA_PUBLIC_KEY_2 = '<clave nueva>';
--   ALTER USER AP_PIPELINE_SVC UNSET RSA_PUBLIC_KEY;
-- =============================================================================