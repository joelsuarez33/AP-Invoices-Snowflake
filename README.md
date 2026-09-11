# Pipeline ELT incremental de cuentas a pagar

Pipeline diario que ingesta el extracto de partidas individuales de acreedores
(transaccion **FBL1N** de SAP FI-AP), lo modela en Snowflake con dbt y publica un
tablero estatico.

El dataset es sintetico, pero el problema no lo es: el extracto es un **change
feed**, no un snapshot. Cada corrida trae solo lo que cambio ese dia, un mismo
documento vuelve a aparecer cuando se corrige o se compensa, y hay documentos que
llegan con semanas de atraso. Todo el diseno del pipeline sale de eso.

---

## El problema

FBL1N no expone un feed de cambios: expone una pantalla. Lo que se puede exportar
todos los dias es un archivo con las partidas que se movieron, con estas
propiedades:

| Propiedad | Consecuencia en el diseno |
|---|---|
| El mismo `BELNR` vuelve a aparecer con datos distintos | La clave del merge es `BELNR`, y RAW guarda todas las versiones |
| Un documento se re-emite completo al compensarse | El estado se resuelve con `QUALIFY`, no con un snapshot diario |
| Llegan documentos con `BUDAT` de 15 a 45 dias atras | El filtro incremental no puede ser por fecha contable |
| `VERZN` lo calcula el report al ejecutarse | El aging se recalcula; el campo de origen no es confiable |
| Tres monedas de documento, una moneda local | Ninguna metrica agregada usa el importe en moneda de documento |
| Fila de totales al pie del archivo | Da un checksum gratis, y hay que sacarla antes de cargar |

---

## Arquitectura

```
generate_extract.py  ->  landing/FBL1N_YYYYMMDD.xlsx       change feed diario
normalize.py         ->  staging_files/FBL1N_YYYYMMDD.csv  desempaqueta + valida checksum
load.py              ->  PUT + COPY INTO                   ingesta append-only a RAW
dbt build            ->  STG -> INT -> MART                 casteo, enriquecido, merge
export_mart.py       ->  web/public/data/*.json            serving estatico
```

Orquestacion: GitHub Actions con cron diario. Sin Airflow, sin Dagster, sin dlt.
Para un pipeline de una sola fuente y cinco pasos, un scheduler dedicado agrega
mas superficie de operacion que valor.

### Las cuatro decisiones que sostienen el resto

**1. RAW es append-only.** Nunca se borra ni se sobrescribe. Cada `COPY INTO`
agrega las filas del feed con su metadata (`_source_file`, `_file_row_number`,
`_loaded_at`, `_extract_date`). Un `BELNR` termina con varias filas: la version
original y cada re-emision. Ese historial es lo que reemplaza a los snapshots
completos y lo que hace auditable el merge. La idempotencia la da Snowflake:
`COPY INTO` registra los archivos ya cargados y los ignora durante 64 dias, asi
que no hay deduplicacion propia en la ingesta.

**2. El casteo vive solo en staging.** Todas las columnas de negocio en RAW son
`STRING`. Una carga no puede fallar porque un campo vino con formato raro: tiene
que aterrizar y quedar visible. `stg_fbl1n__items` es el unico lugar que castea, y
es tambien donde el `QUALIFY` resuelve el change feed a estado actual quedandose
con la ultima version de cada `BELNR`.

**3. El filtro incremental es por `_extract_date`, no por `BUDAT`.** Filtrar por
fecha contable seria lo natural y esta mal: los late-arriving documents traen
`BUDAT` de hasta 45 dias atras y quedarian afuera para siempre. `_extract_date` es
cuando el pipeline **vio** el dato, no cuando ocurrio el hecho, y por eso es
monotona. El modelo lo documenta en el propio SQL.

**4. Todo se mide contra la fecha de corte del ledger, nunca contra
`CURRENT_DATE`.** La fecha de corte es `max(_extract_date)`. Consecuencia:
re-ejecutar el pipeline sobre datos de ayer devuelve exactamente lo que devolvio
ayer.

---

## Modelo dbt

```
seeds       vendors, fx_rates, document_types
staging     stg_fbl1n__items          view. Unico casteo. QUALIFY = estado actual.
            stg_ap__vendors / __fx_rates / __document_types
snapshots   snap_ap_amounts           SCD2 sobre WRBTR y DMBTR (correcciones de importe)
intermediate int_ap_items_enriched    join a los tres maestros + fecha de corte
            int_ap_aging              buckets recalculados, con dias negativos
            int_ap_fx_revaluation     exposicion cambiaria de las abiertas en USD/EUR
marts       fct_ap_open_items         incremental merge, unique_key = BELNR
            fct_ap_payment_performance dias reales vs. condicion pactada
            dim_vendor, dim_date
exposures   ap_dashboard              el tablero de Vercel y los modelos que consume
```

**Por que un solo snapshot.** `snap_ap_amounts` captura las correcciones de
importe como SCD2, que es la unica dimension del ledger cuya vigencia no esta en
los datos. Las compensaciones no lo necesitan: `AUGDT` ya es la fecha del hecho.

**La metrica que no esta en el origen.** `int_ap_fx_revaluation` compara el
importe con el que se contabilizo cada factura en moneda extranjera contra lo que
representa la misma obligacion a la cotizacion de la fecha de corte. En un ledger
que cierra en pesos, esa diferencia es plata y no aparece en ninguna columna de
FBL1N.

**Tests.** 142 en total. `unique` y `not_null` en claves, `relationships` a las
dimensiones, `accepted_values` en `BLART`, `WAERS` y `HWAER`, freshness sobre RAW,
y cuatro tests singulares con `severity` y `store_failures` explicitos:

| Test | Que protege |
|---|---|
| `assert_posting_date_after_document_date` | `BUDAT >= BLDAT` en toda fila |
| `assert_no_verzn_on_cleared_items` | `VERZN` poblado si y solo si la partida esta abierta |
| `assert_mart_reconciles_with_raw` | La mart cuadra contra RAW deduplicado, con tolerancia de un centavo |
| `assert_clearing_date_not_after_extract` | Ningun `AUGDT` posterior a su `_extract_date` |

El tercero es el que detecta el modo de falla mas caro del diseno: un acreedor
nuevo que el maestro todavia no tiene hace que el `inner join` de la capa
intermedia descarte filas en silencio, y el total deja de cuadrar.

---

## Puesta en marcha

### 1. Snowflake

Generar el par de claves RSA **fuera del repo** (`*.p8` y `*.pem` estan en
`.gitignore`):

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Ejecutar `sql/00_setup_account.sql` con `ACCOUNTADMIN`, reemplazando
`<<RSA_PUBLIC_KEY>>` por el contenido de `rsa_key.pub` sin las lineas
`BEGIN`/`END`. Despues `sql/01_setup_raw.sql` con el rol `AP_PIPELINE`.

### 2. Entorno local

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Completar `.env`. No hay variable de password: la autenticacion es por key-pair.

### 3. Primera corrida

```bash
python normalize.py                          # valida y desempaqueta el extracto inicial
python load.py                               # PUT + COPY INTO a RAW
cd dbt_ap && dbt deps && dbt build && cd ..  # seeds, modelos, snapshot y tests
python export_mart.py                        # datasets del tablero
cd web && npm install && npm run dev         # tablero en localhost:3000
```

### 4. Generar mas dias

El extracto inicial deja el ledger al **2026-09-06**. El generador nunca emite un
archivo con fecha posterior a la fecha del sistema, asi que el primer feed
disponible es el dia siguiente.

```bash
python generate_extract.py                   # el dia siguiente a last_run_date
python generate_extract.py --date 2026-09-07  # una fecha puntual
python generate_extract.py --days 60          # 60 dias consecutivos
python generate_extract.py --until 2026-11-05 # hasta esa fecha inclusive
```

`normalize.py` y `load.py` sin argumentos procesan todo lo pendiente en orden
cronologico: un backfill de 60 dias entra en una invocacion de cada uno.

El generador es **idempotente por fecha**: `landing/` es la fuente de verdad y el
universo de documentos se reconstruye replicando los extractos anteriores, con el
RNG sembrado por `(semilla, fecha)`. La misma fecha produce el mismo archivo.

---

## Variables de entorno

Todas se leen de `.env` (local) o del entorno del runner (CI). Ninguna tiene
valor por defecto en el codigo: si falta una, el proceso aborta antes de
conectarse.

| Variable | Que es |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Identificador de cuenta |
| `SNOWFLAKE_USER` | Usuario de servicio (`AP_PIPELINE_SVC`) |
| `SNOWFLAKE_ROLE` | `AP_PIPELINE` |
| `SNOWFLAKE_WAREHOUSE` | `AP_WH` |
| `SNOWFLAKE_DATABASE` | `AP_ANALYTICS` |
| `SNOWFLAKE_SCHEMA_RAW` | `RAW` |
| `SNOWFLAKE_SCHEMA_ANALYTICS` | `ANALYTICS` |
| `SNOWFLAKE_STAGE` | `STG_FBL1N` |
| `SNOWFLAKE_FILE_FORMAT` | `FF_FBL1N_CSV` |
| `SNOWFLAKE_RAW_TABLE` | `FBL1N_ITEMS` |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | Ruta al `.p8` (modo local) |
| `SNOWFLAKE_PRIVATE_KEY` | PEM completo en una variable (modo CI) |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | Vacio si la clave se genero sin cifrar |

`SNOWFLAKE_PRIVATE_KEY_PATH` y `SNOWFLAKE_PRIVATE_KEY` son excluyentes: definir
las dos es un error explicito.

---

## Seguridad

- Cero credenciales en el codigo, en comentarios, como default de
  `os.environ.get()` o en este README.
- `.env` en `.gitignore`; `.env.example` solo con placeholders vacios.
- `profiles.yml`: todos los campos por `env_var()` sin fallback.
- Autenticacion por key-pair RSA. El usuario de servicio se crea con
  `TYPE = SERVICE` y no acepta password.
- En Actions los secrets se mapean a `env:` del step y nunca se imprimen: el
  unico paso que los mira solo publica si estan o no estan.
- `.gitignore` cubre `.env`, `*.p8`, `*.pem`, los extractos generados,
  `staging_files/`, `dbt_ap/target/`, `dbt_ap/logs/`, `dbt_ap/dbt_packages/`,
  `web/node_modules/` y `web/.next/`.

---

## CI/CD

**`ci.yml`** (pull request): `ruff`, `sqlfluff`, `dbt deps` y `dbt parse`, todo
sin tocar Snowflake. `sqlfluff` usa el templater jinja y carga los macros del
proyecto desde disco, asi que no necesita conexion ni resolver paquetes. El paso
de `dbt compile` queda condicionado a que haya credenciales, porque compile si
abre conexion: para resolver `is_incremental()` tiene que preguntarle al
warehouse si la relacion existe.

**`pipeline.yml`** (push a `main` y cron `0 9 * * 1-5`): restaura el estado desde
la rama `artifacts`, genera, normaliza, carga, corre `dbt build`, exporta los
datasets y publica `_state.json`, `run_results.json` y `manifest.json` de vuelta
en `artifacts`. Ese commit persiste el estado para la corrida siguiente, habilita
Slim CI con `--select state:modified+`, y mantiene vivo el cron -- GitHub
deshabilita los schedules despues de 60 dias sin actividad en el repo.

Una sola conexion a Snowflake por corrida: cada resume del warehouse factura un
minimo de 60 segundos.

---

## Estructura

```
.
|-- config.py                 contrato del layout, rutas y lectura de entorno
|-- generate_extract.py       change feed diario (--date / --days / --until)
|-- normalize.py              xlsx -> csv, valida contrato y checksum
|-- load.py                   PUT + COPY INTO append-only
|-- export_mart.py            marts -> JSON estatico
|-- landing/                  extractos xlsx + _state.json
|-- staging_files/            csv normalizados
|-- scripts/enrich_baseline.py  procedencia del dataset inicial (fuera del pipeline)
|-- sql/                      setup de cuenta, RAW, copy de referencia y teardown
|-- dbt_ap/                   proyecto dbt
|-- web/                      tablero Next.js (app router, export estatico)
|-- .github/workflows/        ci.yml y pipeline.yml
`-- docs/DECISIONS.md         indice de ADRs
```

`scripts/enrich_baseline.py` no forma parte del pipeline: documenta como se
construyo el extracto inicial de 5.000 partidas y se versiona para que el dataset
tenga procedencia.
