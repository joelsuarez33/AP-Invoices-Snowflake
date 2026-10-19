# Agente de consulta: casos de uso, verificacion y auditoria

El agente responde preguntas sobre el ledger de cuentas a pagar llamando a
herramientas tipadas (`agent/tools.py`). No escribe SQL: elige una herramienta
y sus parametros, y cada herramienta ejecuta una consulta fija y parametrizada
sobre una base DuckDB local (`agent/ledger.py`).

Esa base se arma sin Snowflake: con el estado del ledger que el pipeline
publica en la rama `artifacts` y los seeds del repo. `python -m agent.ledger`
la reconcilia contra `web/public/data/summary.json`, que se exporto desde la
mart de Snowflake al mismo corte. Hoy cuadran los 9 controles al centavo.

```powershell
pip install -r requirements.txt -r requirements-agent.txt
python -m agent.ledger                 # carga y reconcilia contra la mart
python -m agent.chat                   # sesion interactiva
python -m agent.chat --pregunta "..."  # una pregunta y sale
```

Requiere `ANTHROPIC_API_KEY` en `.env`, con alcance de **espacio de trabajo**:
una key de organizacion pide el header `anthropic-workspace-id`. El modelo por
defecto es `claude-haiku-5-5` (`AGENT_MODEL` y `AGENT_EFFORT` lo cambian).

---

## Como verificar una respuesta

Hay tres caminos, de menor a mayor independencia respecto del agente:

| Camino | Comando | Que demuestra |
|---|---|---|
| SQL sobre la base local | `python -m agent.ledger --sql "..."` | Que la cifra sale de los datos, con una consulta distinta a la de la herramienta |
| Excel | `python -m agent.ledger --export` -> `agent/data/ledger_AAAAMMDD.xlsx` | Lo mismo, con una herramienta que no comparte codigo con el agente |
| Snowflake | Consultas sobre `AP_ANALYTICS.MARTS.FCT_AP_OPEN_ITEMS` | Que la capa local reproduce la mart de dbt (requiere warehouse activo) |

El Excel tiene una fila por documento. Columnas de la `A` a la `Y`:

| Col | Campo | Col | Campo | Col | Campo |
|---|---|---|---|---|---|
| A | belnr | J | budat | S | dmbtr (importe ARS) |
| B | lifnr | K | netdt (vencimiento) | T | hwaer |
| C | vendor_name | L | augdt (fecha de pago) | U | fx_rate_at_document_date |
| D | country | M | is_open | V | fx_rate_at_cutoff |
| E | payment_terms | N | days_overdue | W | amount_local_at_cutoff |
| F | agreed_payment_days | O | aging_bucket | X | fx_revaluation_local |
| G | blart | P | is_within_terms | Y | ledger_cutoff_date |
| H | document_type_description | Q | waers (moneda) | | |
| I | bldat | R | wrbtr (importe moneda doc.) | | |

Las formulas usan la sintaxis de Excel en espanol (`;` como separador). Los
importes de factura son negativos: es deuda con el proveedor.

---

## Los 5 casos

Valores esperados con el ledger al **2026-10-06** (huella del dataset
`1868be9cd07b51cd...`). Si los datos se actualizan, cambian los valores pero no
el procedimiento.

### 1. Deuda total y vencida

> "Cuanto debemos en total a proveedores y cuanto de eso esta vencido?"

Herramienta: `resumen_ledger()`.

| Dato | Esperado |
|---|---|
| Partidas abiertas | 3.631 |
| Deuda abierta | $ 15.781.564.259,21 |
| Partidas vencidas | 2.753 |
| Deuda vencida | $ 12.032.102.249,53 |

SQL local:

```sql
select
    count(*)   filter (where is_open)                     as partidas_abiertas,
    sum(dmbtr) filter (where is_open)                     as deuda_abierta,
    count(*)   filter (where is_open and days_overdue > 0) as partidas_vencidas,
    sum(dmbtr) filter (where is_open and days_overdue > 0) as deuda_vencida
from items
```

Excel:

```
Deuda abierta:     =SUMAR.SI.CONJUNTO(S:S;M:M;VERDADERO)
Deuda vencida:     =SUMAR.SI.CONJUNTO(S:S;M:M;VERDADERO;N:N;">0")
Partidas abiertas: =CONTAR.SI.CONJUNTO(M:M;VERDADERO)
```

Snowflake:

```sql
select count_if(is_open), sum(iff(is_open, dmbtr, 0)),
       count_if(is_open and days_overdue > 0), sum(iff(is_open and days_overdue > 0, dmbtr, 0))
from AP_ANALYTICS.MARTS.FCT_AP_OPEN_ITEMS;
```

### 2. Proveedores con mas deuda vencida

> "Cuales son los 5 proveedores con mas deuda vencida?"

Herramienta: `ranking_acreedores(criterio='saldo_vencido', limite=5)`.

| # | Proveedor | Vencido | % del vencido total |
|---|---|---|---|
| 1 | PROVEEDOR 002 SA (4040) | $ 3.453.907.578,22 | 28,7 % |
| 2 | PROVEEDOR 017 SA (2933) | $ 1.471.364.986,70 | 12,2 % |
| 3 | PROVEEDOR 013 SA (2306) | $ 916.308.491,08 | 7,6 % |
| 4 | PROVEEDOR 040 SA (9930) | $ 895.538.008,57 | 7,4 % |
| 5 | PROVEEDOR 011 SA (2114) | $ 452.571.180,18 | 3,8 % |

SQL local:

```sql
select lifnr, vendor_name, sum(dmbtr) as vencido
from items
where is_open and days_overdue > 0
group by lifnr, vendor_name
order by vencido          -- el mas negativo es la mayor deuda
limit 5
```

Excel: agregar una columna auxiliar `Z` con `=Y(M2;N2>0)`. Tabla dinamica con
filtro `Z = VERDADERO`, filas `vendor_name`, valores `Suma de dmbtr`, orden
ascendente. Para el porcentaje: cada fila dividida por el resultado de
`=SUMAR.SI.CONJUNTO(S:S;M:M;VERDADERO;N:N;">0")`.

### 3. Comportamiento de un proveedor

> "El proveedor 017 paga en termino? como viene su deuda?"

Herramientas: `buscar_acreedor('017')` -> `detalle_acreedor('2933')`. Es el caso
que muestra que el agente encadena llamadas: el usuario da un nombre, no un
codigo.

| Dato | Esperado |
|---|---|
| Partidas pagadas | 469 |
| Pagadas en termino | 13,9 % |
| Dias promedio de pago (desde el documento) | 46,1 |
| Saldo abierto | $ 1.906.693.658,98 |
| Saldo con mas de 90 dias | $ 989.928.117,63 (51,9 %) |

SQL local. "En termino" se escribe como `augdt <= netdt`, no con la columna
`is_within_terms` de la herramienta, para que la verificacion no repita su
logica:

```sql
select count(*)                                          as pagadas,
       round(100.0 * count_if(augdt <= netdt) / count(*), 1) as pct_en_termino,
       round(avg(augdt - bldat), 1)                      as dias_promedio_pago
from items where lifnr = '2933' and not is_open;

select sum(dmbtr) filter (where date '2026-10-06' - netdt > 90) as abierto_90_mas,
       sum(dmbtr)                                               as abierto_total
from items where lifnr = '2933' and is_open;
```

Excel (columna auxiliar `Z` con `=SI(M2;"";L2-I2)`, los dias de pago):

```
Pagadas:            =CONTAR.SI.CONJUNTO(B:B;"2933";M:M;FALSO)
En termino:         =CONTAR.SI.CONJUNTO(B:B;"2933";M:M;FALSO;P:P;VERDADERO) / [pagadas]
Dias de pago:       =PROMEDIO.SI.CONJUNTO(Z:Z;B:B;"2933";M:M;FALSO)
Abierto mas de 90:  =SUMAR.SI.CONJUNTO(S:S;B:B;"2933";M:M;VERDADERO;N:N;">90")
```

### 4. Perdida por tipo de cambio

> "Cuanto estamos perdiendo por tipo de cambio y en que moneda?"

Herramienta: `exposicion_cambiaria()`.

| Moneda | Partidas abiertas | Revaluacion | % |
|---|---|---|---|
| USD | 747 | -$ 192.423.159,51 | 55,2 % |
| EUR | 381 | -$ 156.143.287,05 | 44,8 % |

La verificacion recalcula la revaluacion desde cero: importe en moneda del
documento por la cotizacion de la fecha de corte, menos lo contabilizado en
pesos. No usa la columna `fx_revaluation_local` que calculo la base:

```sql
select i.waers, count(*) as partidas,
       sum(round(i.wrbtr * r.rate_to_local - i.dmbtr, 2)) as revaluacion
from items i
join fx_rates r on r.currency = i.waers and r.rate_date = date '2026-10-06'
where i.is_open and i.waers <> 'ARS'
group by i.waers
```

Excel (columna auxiliar `Z` con `=SI(Y(M2;Q2<>"ARS");REDONDEAR(R2*V2-S2;2);"")`):

```
USD: =SUMAR.SI.CONJUNTO(Z:Z;Q:Q;"USD")
EUR: =SUMAR.SI.CONJUNTO(Z:Z;Q:Q;"EUR")
```

Snowflake, recalculando desde el seed de cotizaciones:

```sql
select f.waers, sum(round(f.wrbtr * r.rate_to_ars - f.dmbtr, 2))
from AP_ANALYTICS.MARTS.FCT_AP_OPEN_ITEMS f
join AP_ANALYTICS.SEEDS.FX_RATES r on r.currency = f.waers and r.rate_date = '2026-10-06'
where f.is_open and f.waers <> 'ARS'
group by f.waers;
```

### 5. Partidas puntuales

> "Cuantas facturas en dolares tienen mas de 90 dias de atraso? mostrame las 3 de mayor importe"

Herramienta: `buscar_partidas(moneda='USD', tramo='90+', orden='importe_moneda_doc', limite=3)`.

420 partidas. Las 3 mayores en dolares:

| Documento | Proveedor | USD | ARS | Dias |
|---|---|---|---|---|
| 8247916440 | PROVEEDOR 001 SA | 16.180,46 | 25.000.000 | 201 |
| 2216096617 | PROVEEDOR 002 SA | 16.165,96 | 25.000.000 | 210 |
| 3759360694 | PROVEEDOR 030 SA | 16.165,96 | 25.000.000 | 206 |

Los $ 25.000.000 son el tope por documento del generador sintetico
(`AMOUNT_CAP`). Cada factura se convirtio a la cotizacion de su fecha, y por
eso el mismo importe en pesos da importes distintos en dolares.

```sql
select count(*) from items
where is_open and waers = 'USD' and date '2026-10-06' - netdt > 90;

select belnr, vendor_name, wrbtr, dmbtr, days_overdue from items
where is_open and waers = 'USD' and days_overdue > 90
order by abs(wrbtr) desc, belnr
limit 3;
```

Excel: `=CONTAR.SI.CONJUNTO(M:M;VERDADERO;Q:Q;"USD";N:N;">90")`. Para el top 3,
filtrar `is_open = VERDADERO`, `waers = USD`, `aging_bucket = 90+` y ordenar
`wrbtr` de menor a mayor (el mas negativo es la factura mas grande).

---

## Auditoria

Cada pregunta deja una linea en `agent/logs/audit.jsonl` con la pregunta, la
respuesta completa, el modelo, la huella del dataset, cada herramienta llamada
con sus argumentos, el SQL que ejecuto (plantilla, parametros y version lista
para copiar), las filas devueltas, un SHA-256 de cada resultado y los tokens.

```powershell
python -m agent.audit list           # preguntas registradas
python -m agent.audit show <id>      # pregunta, SQL ejecutado y respuesta
python -m agent.audit replay <id>    # re-ejecuta el SQL y compara los hashes
```

El id aparece al final de cada respuesta del chat. `show` imprime el SQL
renderizado, listo para pegar en `python -m agent.ledger --sql`.

## Idempotencia

Lo que tiene que ser identico entre corridas es lo que esta debajo del texto:

1. Hacer la misma pregunta varias veces (`--pregunta` la deja registrada).
2. `python -m agent.audit list`: las corridas llaman a las mismas herramientas.
3. `python -m agent.audit replay <id>` sobre cada una: mismo SQL, mismas filas,
   mismo hash del resultado, sobre el mismo dataset (misma huella).

La redaccion de la respuesta puede variar, porque es un modelo de lenguaje.
Lo que no puede variar son las cifras, y el replay es la prueba.

Hubo un caso que lo demostro. La pregunta 5 corrida dos veces con la primera
version de `buscar_partidas` dio dos listados distintos: la herramienta solo
ordenaba por importe en pesos, y en la segunda corrida el modelo pidio 100
filas y las reordeno en dolares por su cuenta. El replay mostro que el SQL de
las dos corridas era identico: la diferencia estaba en el modelo, no en los
datos. La correccion fue darle a la herramienta el orden `importe_moneda_doc`
y prohibir en el prompt ordenar o contar por cuenta propia. Desde entonces las
corridas repetidas hacen la misma llamada y devuelven las mismas filas.
