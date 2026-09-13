# Registro de decisiones

Once decisiones del pipeline, cada una con contexto, decision, alternativas
descartadas y consecuencias. Las ocho primeras son de diseno. Las tres ultimas
salen de fallas que detectaron los tests de dbt.

Los numeros son de Snowflake al 2026-09-13, con el ledger al 2026-09-12: 67
extractos diarios, 9.023 filas en RAW, 7.002 documentos.

| ADR | Decision |
|---|---|
| 001 | El extracto es un change feed, no un snapshot |
| 002 | El estado actual se resuelve con `QUALIFY` en staging |
| 003 | El filtro incremental va por `_extract_date`, no por `BUDAT` |
| 004 | RAW append-only como historia, y un unico snapshot para importes |
| 005 | `VERZN` no se usa para medir: el aging se recalcula |
| 006 | La fila de totales se usa como checksum y nunca llega a RAW |
| 007 | Toda metrica agregada va en `DMBTR`; la revaluacion es un modelo propio |
| 008 | `COPY INTO` desde stage interno, sin conector gestionado |
| 009 | Falla: `relationships` de `lifnr`, 4.518 filas, seed desactualizado |
| 010 | Falla: `not_null` de `fx_rate_at_document_date`, 754 filas, serie FX incompleta |
| 011 | Falla: el snapshot no guarda historia de un backfill |

---

## ADR-001 — El extracto es un change feed, no un snapshot

### Contexto

FBL1N es un report de pantalla, no una tabla con marca de modificacion. Lo que
se puede exportar por dia es el conjunto de partidas que se movieron. Cada
extracto del dataset trae:

- 20 a 40 documentos creados ese dia.
- 20 a 40 partidas compensadas, re-emitidas completas, con `AUGDT` y `AUGBL`
  poblados y `VERZN` vacio.
- Alrededor de 0,5% de correcciones de importe sobre `BELNR` existentes.
- Alrededor de 2% de documentos que llegan tarde, con `BUDAT` de 15 a 45 dias
  atras.

En un SAP real ese delta se arma con dos extracciones: `BSIK` por fecha de
entrada y `BSAK` por `AUGDT` dentro de la ventana.

### Decision

Trato cada archivo como un change feed. Un mismo `BELNR` puede aparecer en
varios extractos, y la version valida es la del extracto mas reciente. La
ingesta y el modelado (ADR-002 a ADR-004) parten de esa premisa.

### Alternativas descartadas

- **Delta de solo altas.** No funciona: la compensacion es un UPDATE sobre una
  fila que ya esta en el warehouse. Con solo altas, una factura pagada queda
  abierta para siempre.
- **Snapshot completo diario.** Resuelve el estado sin merge, pero reprocesa el
  ledger entero cada dia (7.002 documentos contra 46 a 78 filas por feed) y
  repite en RAW informacion que no cambio.

### Consecuencias

- El volumen diario es chico. Los extractos del 07/09 al 12/09 trajeron 61, 66,
  50, 78, 51 y 46 filas.
- Hay que resolver versiones en algun lugar (ADR-002). En RAW, 2.012 de los
  7.002 documentos tienen mas de una version.
- La fecha del extracto sale del nombre del archivo (`FBL1N_YYYYMMDD`), no de
  la hora de carga. Regenerar o recargar un dia no le cambia la fecha.

---

## ADR-002 — El estado actual se resuelve con `QUALIFY` en staging

### Contexto

RAW guarda una fila por cada aparicion de un documento (ADR-004). Alguna capa
tiene que colapsar esas versiones al estado actual antes de que lo consuman la
capa intermedia, la mart y el snapshot.

### Decision

`stg_fbl1n__items` es el unico lugar donde se resuelve el feed y el unico que
castea tipos:

```sql
qualify row_number() over (
    partition by belnr
    order by _extract_date desc, _file_row_number desc
) = 1
```

`_file_row_number` solo desempata si un `BELNR` aparece dos veces en el mismo
archivo. El generador garantiza que no pasa, pero un extracto real no ofrece
esa garantia, y la resolucion no puede quedar indefinida.

El generador aplica la misma regla, ultima version gana, cuando reconstruye su
universo de documentos. `assert_mart_reconciles_with_raw` la reescribe contra
la source en lugar de hacer `ref()` a staging. Si referenciara staging, el
test compararia el modelo consigo mismo.

### Alternativas descartadas

- **`MERGE` sobre RAW en la ingesta.** Pierde la historia, y un error en la
  clave del merge no tiene vuelta atras.
- **El snapshot de dbt como fuente del estado actual.** Registra el estado solo
  en cada invocacion. Un backfill cargado en un lote queda como una sola
  version (ADR-011).
- **Resolver en cada modelo de la mart.** Cada modelo tendria que reimplementar
  la regla, y tarde o temprano dos modelos la implementarian distinto.

### Consecuencias

- `unique_stg_fbl1n__items_belnr` pasa con 2.012 documentos multi-version.
- La vista recalcula la ventana sobre todo RAW en cada consulta. Con 9.023
  filas no pesa. Si RAW creciera varios ordenes de magnitud, el paso siguiente
  seria materializar staging como incremental.
- La regla existe en tres lugares: staging, el generador y el test de
  reconciliacion. Si alguno cambia sin los otros, la reconciliacion falla.

---

## ADR-003 — El filtro incremental va por `_extract_date`, no por `BUDAT`

### Contexto

`fct_ap_open_items` es un modelo incremental con `merge` y
`unique_key = 'belnr'`. Lo natural es procesar lo contabilizado desde la ultima
corrida, pero cerca del 2% de cada feed son documentos con `BUDAT` de 15 a 45
dias atras. Entre el 07/09 y el 12/09 llegaron entre uno y dos por dia.

### Decision

```sql
where _extract_date > (select max(_extract_date) from {{ this }})
   or is_open
```

`_extract_date` es cuando el pipeline vio la fila, no cuando ocurrio el hecho.
Es monotona por construccion. La segunda condicion re-mergea las partidas
abiertas en cada corrida: su aging depende de la fecha de corte, que avanza
aunque el documento no cambie. La tabla guarda el ledger completo con un flag
`is_open`. Si filtrara las compensadas, la fila abierta vieja de una partida
pagada quedaria en la tabla para siempre, porque el merge actualiza y agrega
pero no borra lo que dejo de aparecer.

### Alternativas descartadas

- **`budat > max(budat)`.** Descarta en silencio los documentos que llegan
  tarde, y el faltante aparece meses despues al conciliar contra SAP.
- **`BUDAT` con ventana de 45 dias.** Funciona mientras la demora sea menor que
  la ventana. Un documento con 46 dias de atraso se pierde igual, y la ventana
  se re-escanea todos los dias.
- **`_loaded_at`.** Tambien es monotona, pero el orden de versiones en staging
  va por `_extract_date`. Filtrar por un reloj y ordenar por otro abre casos
  donde los dos no coinciden.
- **Tabla completa en cada corrida.** Con este volumen cuesta lo mismo. Mantengo
  el incremental porque en un ledger real el volumen es otro, y el criterio del
  filtro es lo que tiene que estar resuelto.

### Consecuencias

- Un documento que llega tarde entra a la mart el dia que llega.
- Cada corrida re-mergea las 3.631 partidas abiertas.
- Un extracto cargado fuera de orden, con fecha anterior al maximo de la mart,
  no entra por el filtro. `assert_mart_reconciles_with_raw` lo detecta por
  diferencia de cantidad de partidas, y se corrige con
  `dbt build --full-refresh --select fct_ap_open_items`.
- `dbt_created_at` esta en `merge_exclude_columns`: conserva cuando la fila
  entro por primera vez a la mart.

---

## ADR-004 — RAW append-only como historia, y un unico snapshot para importes

### Contexto

Necesito poder auditar como llego cada documento a su estado actual,
incluidas las correcciones de importe. Hay dos formas de guardar esa historia:
en la zona de aterrizaje o en snapshots SCD2 del ledger.

### Decision

RAW nunca borra ni sobrescribe. Cada `COPY INTO` agrega filas con
`_source_file`, `_file_row_number`, `_loaded_at` y `_extract_date`. Todas las
columnas de negocio son `STRING`, y los casteos viven en las macros de
`sap_casts.sql`, que solo se usan en staging.

Hay un solo snapshot, `snap_ap_amounts`, con `strategy='check'` sobre `wrbtr` y
`dmbtr`. El importe es el unico atributo cuya vigencia no esta en los datos.
La compensacion no lo necesita: `AUGDT` ya es la fecha del hecho.

### Alternativas descartadas

- **Upsert por `BELNR` en RAW.** Pierde las versiones y deja sin base al test de
  reconciliacion.
- **Snapshot completo del ledger.** Repite lo que RAW ya guarda con
  `_extract_date`.
- **`strategy='timestamp'`.** El origen no tiene una columna de modificacion
  confiable. `VERZN` cambia todos los dias, asi que generaria una version diaria
  por cada partida abierta.
- **Tipar RAW.** Un valor con formato inesperado haria fallar la carga en lugar
  de aterrizar y quedar visible.

### Consecuencias

- RAW tiene 9.023 filas para 7.002 documentos.
- El snapshot tiene 7.004 filas y 2 versiones cerradas, que corresponden a las
  dos correcciones de importe del 07/09 y del 11/09.
- La historia del snapshot empieza con las corridas del workflow. Lo anterior
  solo esta en RAW (ADR-011).

---

## ADR-005 — `VERZN` no se usa para medir: el aging se recalcula

### Contexto

`VERZN` es la demora tras el vencimiento neto, calculada por el report al
momento de ejecutarse. El mismo documento trae un valor distinto en cada
extracto, y solo esta poblado mientras la partida esta abierta: una partida
compensada se re-emite con `VERZN` vacio. No es un hecho del documento, es un
calculo de presentacion.

### Decision

Staging expone la columna como `verzn_report_unreliable`. El aging se calcula
en `int_ap_aging` contra la fecha de corte del ledger, `max(_extract_date)`,
nunca contra `CURRENT_DATE`:

```sql
datediff('day', netdt, coalesce(augdt, ledger_cutoff_date)) as days_overdue
```

Los buckets admiten dias negativos (`No vencida`, `Vence hoy`, `1-30` ... `90+`).
La columna llega a la mart solo para explicar la diferencia contra la pantalla
de SAP, junto con `verzn_drift_days`. Ninguna metrica la usa.
`assert_no_verzn_on_cleared_items` verifica que este poblada si y solo si la
partida esta abierta.

### Alternativas descartadas

- **Usar `VERZN` tal cual.** Depende de cuando se ejecuto el report. Un
  documento sin re-emitir desde hace semanas conserva la demora de ese dia, y
  en las compensadas esta vacio.
- **Descartar la columna en staging.** Pierdo la forma de explicar por que la
  mart no coincide con lo que ve el analista en la transaccion.
- **Medir contra `CURRENT_DATE`.** Re-procesar los datos de ayer daria un
  resultado distinto cada dia.

### Consecuencias

- Correr el pipeline dos veces sobre los mismos datos devuelve el mismo aging.
- El nombre de la columna avisa que no es confiable a quien la encuentre.
- Los buckets negativos muestran los pagos anticipados. Un bucket que arranque
  en cero los esconderia.

---

## ADR-006 — La fila de totales se usa como checksum y nunca llega a RAW

### Contexto

SAP agrega al pie del report una fila de totales. En `ICON_STATUS` trae el
marcador `@5C\QPendientes@`, en `DMBTR` la suma en moneda local y en `WRBTR` un
`*` literal, porque SAP no suma importes de monedas distintas. Es presentacion,
no un documento. Si se cargara, duplicaria el total del ledger.

### Decision

`normalize.py` valida cada archivo antes de escribir el CSV:

1. Los 21 encabezados, en el orden de `config.COLUMNS`.
2. Exactamente una fila de totales.
3. La suma de `DMBTR` del detalle contra el `DMBTR` de la fila de totales, con
   tolerancia de 0,01 ARS. Si la diferencia es mayor, rechaza el archivo entero.

Si `WRBTR` en la fila de totales no es `*`, lo registra como warning y no
falla: SAP imprime un numero cuando todas las partidas comparten moneda. La
fila de totales nunca se escribe en el CSV.

### Alternativas descartadas

- **Filtrar la fila en staging.** RAW contendria una fila que no es un hecho, y
  el control correria despues de la carga. Un archivo truncado ya estaria en
  una tabla append-only de la que no se borra.
- **Checksum tambien sobre `WRBTR`.** No es sumable entre monedas. SAP tampoco
  lo suma.
- **Rechazar solo las filas que no cuadran.** Un total no dice que fila esta
  mal, y una carga parcial esconderia un archivo truncado.

### Consecuencias

- Un export cortado o editado a mano falla antes de tocar Snowflake, y no hay
  nada que limpiar en RAW.
- El checksum usa la misma tolerancia que `assert_mart_reconciles_with_raw`:
  el pipeline cuadra con el mismo criterio a la entrada y a la salida.
- Un cambio de variante de layout en SAP rompe en `normalize.py` y no seis
  modelos mas abajo.
- Los seis extractos del 07/09 al 12/09 cuadraron con diferencia 0,0000.

---

## ADR-007 — Toda metrica agregada va en `DMBTR`; la revaluacion es un modelo propio

### Contexto

La moneda local es ARS. Los documentos estan en ARS (70%), USD (20%) y EUR
(10%). `DMBTR` es el importe de referencia y `WRBTR = DMBTR / cotizacion` a la
fecha del documento. Sumar `WRBTR` entre documentos mezcla pesos, dolares y
euros en un solo numero.

### Decision

Todas las metricas agregadas usan `DMBTR`. `WRBTR` queda en la mart para
trazar contra el documento original, y solo se suma dentro de una misma moneda.

La exposicion cambiaria vive en `int_ap_fx_revaluation`, solo para partidas
abiertas con `waers != hwaer`:

- `amount_local_at_cutoff = WRBTR * cotizacion a la fecha de corte`, tomando
  la ultima cotizacion disponible en o antes del corte.
- `fx_revaluation_local = amount_local_at_cutoff - DMBTR`. Negativo significa
  que el pasivo en pesos crecio.

### Alternativas descartadas

- **Convertir todo a USD.** El ledger cierra en pesos. Agregaria una segunda
  conversion y esconderia la cifra que se contabiliza.
- **Una columna derivada dentro de la mart.** La exposicion aplica a un
  subconjunto (abiertas en moneda extranjera). Como modelo propio tiene sus
  propios tests, como `accepted_values` en `USD` y `EUR`.
- **Revaluar a la cotizacion de `CURRENT_DATE`.** Tiene el mismo problema de
  reproducibilidad que el aging (ADR-005).

### Consecuencias

- Al corte del 12/09 hay 728 partidas abiertas en USD y 373 en EUR. La
  revaluacion suma -182.838.690,63 ARS: -93.865.614,57 de USD y -88.973.076,06
  de EUR. Esa cifra no esta en ninguna columna de FBL1N.
- El export del tablero agrupa `WRBTR` por `waers`, nunca a traves de monedas.
- La revaluacion depende de que la serie FX cubra el ledger completo (ADR-010).

---

## ADR-008 — `COPY INTO` desde stage interno, sin conector gestionado

### Contexto

Hay una sola fuente: un archivo plano por dia, con layout estable, que produce
el propio pipeline. El warehouse es XS, con un resource monitor de 10 creditos
por mes. El costo estimado es de alrededor de USD 5 por mes con corrida diaria.

### Decision

`load.py` hace `PUT` al stage interno `STG_FBL1N` (`AUTO_COMPRESS = TRUE`,
`OVERWRITE = FALSE`) y un solo `COPY INTO` con transformacion, que agrega la
metadata de carga. `_extract_date` se parsea con una regex sobre
`METADATA$FILENAME`. La idempotencia la da el load history del stage, que dura
64 dias: sin `FORCE` y sin deduplicacion propia. Uso una sola conexion por
corrida.

### Alternativas descartadas

- **Fivetran o Airbyte.** Suman un servicio pago o alojado para leer un archivo
  que ya tengo, y un segundo lugar donde se define el esquema. La idempotencia
  no mejora sobre el load history.
- **dlt.** Infiere el esquema y guarda estado del lado de Python. Para un
  archivo por dia reemplaza unas 40 lineas de SQL por una dependencia con estado
  propio.
- **Snowpipe.** Esta pensado para archivos que llegan de forma continua. Con un
  archivo diario y un orquestador que ya existe, agrega objetos de notificacion
  sin ganar latencia.
- **Stage externo en S3.** Suma otra cuenta cloud y otras credenciales.

### Consecuencias

- Verificado el 13/09. Una corrida que fallo despues de la carga se reintento:
  el generador reescribio del 07/09 al 12/09 con los mismos totales al centavo,
  `COPY` no proceso ningun archivo y RAW siguio en 9.023 filas, sin
  duplicados. El extracto versionado del 06/09 se vuelve a normalizar en cada
  corrida, y su `PUT` devuelve `SKIPPED`.
- Ese reintento destapo un bug. Sin archivos nuevos, `COPY` no devuelve cero
  filas: devuelve una fila de una columna (`Copy executed with 0 files
  processed.`), y `load.py` leia `row[1]`. Esta corregido en `309076d`.
- El load history vence a los 64 dias. `OVERWRITE = FALSE` evita volver a
  subir un archivo mientras siga en el stage. Si se borraran archivos (`REMOVE`
  o `PURGE`) y se subieran de nuevo pasada la ventana, `COPY` los cargaria otra
  vez. Por eso no se borran archivos del stage.
- Cada resume del warehouse factura 60 segundos como minimo, y por eso cada
  script abre una sola conexion.

---

## ADR-009 — Falla: `relationships` de `lifnr`, 4.518 filas, seed desactualizado

### Contexto

En el primer `dbt build` completo, el test `relationships` de `lifnr` contra el
maestro de acreedores fallo con 4.518 filas. La tabla `SEEDS.VENDORS` en
Snowflake tenia una version anterior de `vendors.csv`.

Revisar el CSV no alcanzaba para verlo. Los cinco primeros acreedores (2505,
4040, 4650, 6930 y 9059) vienen fijos del extracto original en
`scripts/enrich_baseline.py`, asi que las primeras lineas eran iguales en las
dos versiones.

La causa no fue el modo de carga de `dbt seed`. Sin `--full-refresh`, dbt hace
`truncate` + `insert`: reemplaza los datos y conserva la estructura.
`--full-refresh` (drop + create) solo hace falta cuando cambian columnas o
tipos. La tabla tenia el contenido viejo porque la version nueva del CSV no se
habia sembrado.

### Decision

- Los seeds se cargan en cada corrida. `pipeline.yml` corre `dbt build`, que
  incluye `dbt seed`, asi que el CSV del repo y la tabla no pueden divergir por
  mas de una corrida.
- Un seed se compara por cantidad de filas y por contenido, no por las primeras
  lineas.
- El `relationships` queda con `severity: error` y con `store_failures`, para
  poder consultar los `LIFNR` que fallan.

### Alternativas descartadas

- **`left join` al maestro.** La mart mostraria documentos sin nombre de
  acreedor en lugar de fallar, y el desvio del maestro pasaria desapercibido.
- **Bajar el test a `warn`.** El build publicaria igual y el tablero perderia
  4.518 partidas por el `inner join` de la capa intermedia.

### Consecuencias

- Al 13/09 hay 40 `LIFNR` distintos en RAW y 40 filas en `SEEDS.VENDORS`.
- Como `int_ap_items_enriched` usa `inner join`, un acreedor desconocido
  descarta filas. El `relationships` senala la causa y
  `assert_mart_reconciles_with_raw` mide el impacto.
- Si aparece un acreedor que el seed no tiene, el pipeline queda en rojo hasta
  actualizar el maestro. Es intencional.

---

## ADR-010 — Falla: `not_null` de `fx_rate_at_document_date`, 754 filas, serie FX incompleta

### Contexto

El test `not_null` sobre `fx_rate_at_document_date` en `int_ap_items_enriched`
fallo con 754 filas. La serie de `fx_rates.csv` arrancaba el 2026-03-01 y habia
documentos con `BLDAT` desde el 2026-02-06. El join a la serie es un
`left join` por moneda y fecha, asi que esas filas no desaparecieron: llegaron
con la cotizacion en nulo.

Sin el test, esos documentos habrian llegado a la mart sin la cotizacion que
explica la relacion entre `WRBTR` y `DMBTR`, y con `fx_drift_pct` nulo en los
que estan en USD y EUR. Nada habria fallado.

### Decision

Extendi la serie al rango del 2026-01-01 al 2027-12-31: 2.190 filas, 730 dias
por 3 monedas. Es el mismo horizonte que el calendario de `dim_date`
(`date_spine_start` y `date_spine_end`). El `not_null` queda con
`severity: error`. El generador lee la misma serie y no inventa cotizaciones.

### Alternativas descartadas

- **Tomar la cotizacion mas cercana en el join.** Esconde que la serie no cubre
  el ledger, y la cotizacion a la fecha del documento tiene que ser exacta. La
  de corte si toma la ultima disponible, porque una fecha de corte puede caer
  en un dia sin cotizacion.
- **Bajar el test a `warn`.** El build publicaria con nulos.

### Consecuencias

- Al 13/09, el `BLDAT` minimo en RAW es 2026-02-06, la serie arranca el
  2026-01-01 y la mart tiene 0 filas con cotizacion nula.
- La serie y el calendario terminan el 2027-12-31. Antes de esa fecha hay que
  extender los dos. Si no, el mismo test vuelve a fallar.

---

## ADR-011 — Falla: el snapshot no guarda historia de un backfill

### Contexto

Despues de cargar 61 dias (del 2026-07-08 al 2026-09-06) y correr `dbt build`,
`snap_ap_amounts` tenia 6.827 filas para 6.827 documentos, sin ninguna version
cerrada, aunque RAW si tenia correcciones de importe. Un snapshot de dbt
registra el estado que ve en cada invocacion. El backfill entro en un solo
build, asi que el snapshot vio unicamente el estado final que resuelve el
`QUALIFY`.

### Decision

No reconstruyo la historia SCD2 del backfill. La historia previa vive en RAW,
con todas las versiones y su `_extract_date`. El snapshot acumula desde las
corridas del workflow.

### Alternativas descartadas

- **Replay dia por dia.** 61 builds, cada uno con resume del warehouse, y un
  mecanismo para cargar un extracto por vez. Reproduce la historia a un costo
  unico alto.
- **Escribir el SCD2 a mano desde RAW en la tabla del snapshot.** Escribe en
  una tabla que administra dbt, y `dbt_valid_from` pasaria a significar
  `_extract_date` para lo historico y hora de corrida para lo nuevo: dos
  significados en una misma columna.
- **Un modelo SCD2 derivado de RAW en lugar del snapshot.** Es viable. Lo dejo
  afuera porque RAW ya permite reconstruir la vigencia con una consulta cuando
  haga falta, y tener dos mecanismos SCD2 duplica la definicion.

### Consecuencias

- Al 13/09 el snapshot tiene 7.004 filas y 2 versiones cerradas, que son las
  correcciones del 07/09 y del 11/09.
- `dbt_valid_from` es la hora de la corrida, no la fecha del extracto.
- El mismo limite aplica a cualquier corrida que cubra mas de un dia: la de los
  lunes, que genera viernes, sabado y domingo, o un backfill manual. Si un
  documento se corrige dos veces dentro de esa ventana, el snapshot registra
  solo la ultima version. RAW conserva las dos.
