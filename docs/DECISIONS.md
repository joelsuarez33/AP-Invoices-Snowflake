# Registro de decisiones de arquitectura

Indice de ADRs. Solo los titulos: cada uno se desarrolla cuando la decision se
discuta o se revierta.

## Ingesta

- **ADR-001** — Tratar el extracto de FBL1N como change feed y no como snapshot diario
- **ADR-002** — RAW append-only: conservar todas las versiones de un documento en lugar de sobrescribir
- **ADR-003** — Delegar la idempotencia de la ingesta al load history de `COPY INTO`
- **ADR-004** — Todas las columnas de negocio como `STRING` en RAW; castear solo en staging
- **ADR-005** — Derivar `_extract_date` del nombre del archivo y no de la fecha de carga
- **ADR-006** — Stage interno en lugar de external stage sobre object storage
- **ADR-007** — Validar el checksum contra la fila de totales y rechazar el archivo completo si no cuadra
- **ADR-008** — Calcular el checksum solo sobre el importe en moneda local

## Modelado

- **ADR-009** — Resolver el estado actual con `QUALIFY` sobre RAW en lugar de snapshots completos
- **ADR-010** — Desempatar versiones por `_extract_date` y `_file_row_number`
- **ADR-011** — Tratar `VERZN` como campo de origen no confiable y recalcular el aging
- **ADR-012** — Medir contra la fecha de corte del ledger y nunca contra `CURRENT_DATE`
- **ADR-013** — Filtrar el incremental por `_extract_date` y no por `BUDAT`, por los late-arriving documents
- **ADR-014** — Re-mergear las partidas abiertas en cada corrida para que el aging no quede congelado
- **ADR-015** — Materializar todo el ledger en `fct_ap_open_items` y filtrar por `is_open` en consumo
- **ADR-016** — Un unico snapshot SCD2, limitado a las correcciones de importe
- **ADR-017** — `strategy='check'` y no `'timestamp'`: el origen no tiene columna de modificacion confiable
- **ADR-018** — Prohibir la agregacion de `WRBTR`; toda metrica agregada va en `DMBTR`
- **ADR-019** — Exponer la exposicion cambiaria como modelo propio en vez de como columna derivada
- **ADR-020** — Buckets de aging con dias negativos para no ocultar los pagos anticipados
- **ADR-021** — Conservar los nombres de campo de SAP en staging en lugar de renombrar a ingles
- **ADR-022** — `inner join` a los maestros: fallar visible ante un acreedor desconocido, no descartar en silencio
- **ADR-023** — Maestros como seeds versionados y no como tablas cargadas
- **ADR-024** — Esquemas custom literales (`generate_schema_name` sin prefijo del target)
- **ADR-025** — `dim_date` con `GENERATOR` nativo en lugar de `dbt_utils.date_spine`

## Operacion

- **ADR-026** — GitHub Actions como orquestador, sin Airflow ni Dagster
- **ADR-027** — Una sola conexion a Snowflake por corrida, por el minimo de facturacion de 60 segundos
- **ADR-028** — Publicar `_state.json` en una rama `artifacts` para persistir el estado entre corridas
- **ADR-029** — Usar ese mismo commit para mantener vivo el cron ante la desactivacion a los 60 dias
- **ADR-030** — `landing/` como fuente de verdad del generador y `_state.json` como checkpoint
- **ADR-031** — Sembrar el RNG con `(semilla, fecha)` para hacer el generador idempotente por dia
- **ADR-032** — Prohibir extractos con fecha futura
- **ADR-033** — Tablero como export estatico de JSON en lugar de consulta directa al warehouse
- **ADR-034** — `sqlfluff` con templater jinja en lugar del templater dbt, para lintear sin conexion
- **ADR-035** — Condicionar `dbt compile` en CI a la disponibilidad de credenciales
- **ADR-036** — Autenticacion por key-pair RSA con dos modos: ruta en disco (local) y PEM en variable (CI)
- **ADR-037** — `env_var()` sin fallback en `profiles.yml`: fallar antes que conectarse a un destino por defecto
- **ADR-038** — `store_failures` activo por defecto en los tests
