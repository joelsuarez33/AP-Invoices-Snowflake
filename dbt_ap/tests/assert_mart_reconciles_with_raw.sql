{{
    config(
        severity = 'error',
        store_failures = true,
        alias = 'fail_mart_raw_reconciliation',
        tags = ['reconciliacion', 'ap']
    )
}}

/*
    Reconciliacion punta a punta: la suma de DMBTR en la mart tiene que cuadrar
    con la suma sobre RAW deduplicado.

    Va contra la source y no contra stg_fbl1n__items a proposito. Comparar la
    mart contra staging seria comparar el modelo consigo mismo: los dos leen la
    misma vista. Reaplicar el QUALIFY sobre RAW, en cambio, verifica de punta a
    punta que ni el merge incremental ni los inner join de la capa intermedia
    perdieron o duplicaron partidas.

    Es el test que detecta el modo de falla mas caro del diseno: un acreedor o
    una clase de documento nuevos que el maestro todavia no tiene hacen que el
    inner join de int_ap_items_enriched descarte filas en silencio. El total
    deja de cuadrar y esto salta.

    Tolerancia: un centavo, que es la misma que usa el checksum de normalize.py
    contra la fila de totales del extracto. El proyecto usa el mismo criterio de
    cuadratura en las dos puntas del pipeline.
*/

{% set tolerance = 0.01 %}

with raw_deduplicated as (

    -- Misma regla de resolucion que stg_fbl1n__items, reescrita a proposito:
    -- si esta consulta hiciera ref() a staging, el test no probaria nada.
    select
        cast(nullif(trim(dmbtr), '') as number(19, 2)) as dmbtr
    from {{ source('sap_fi', 'fbl1n_items') }}
    qualify row_number() over (
            partition by belnr
            order by _extract_date desc, _file_row_number desc
        ) = 1

),

raw_total as (

    select
        count(*)                as raw_items,
        coalesce(sum(dmbtr), 0) as raw_amount_local
    from raw_deduplicated

),

mart_total as (

    select
        count(*)                as mart_items,
        coalesce(sum(dmbtr), 0) as mart_amount_local
    from {{ ref('fct_ap_open_items') }}

),

reconciliation as (

    select
        raw_total.raw_items,
        mart_total.mart_items,
        raw_total.raw_items - mart_total.mart_items               as item_delta,
        raw_total.raw_amount_local,
        mart_total.mart_amount_local,
        raw_total.raw_amount_local - mart_total.mart_amount_local as amount_delta
    from raw_total
    cross join mart_total

)

select *
from reconciliation
where item_delta != 0
    or abs(amount_delta) > {{ tolerance }}
