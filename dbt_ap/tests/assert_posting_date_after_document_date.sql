{{
    config(
        severity = 'error',
        store_failures = true,
        alias = 'fail_budat_before_bldat',
        tags = ['invariantes', 'fbl1n']
    )
}}

/*
    Invariante: BUDAT >= BLDAT en toda fila.

    Un documento no se puede contabilizar antes de existir. Si esto falla, el
    extracto de origen esta corrupto o el generador rompio una invariante: en
    cualquiera de los dos casos el pipeline no debe seguir, porque el aging y
    el analisis de plazos quedan sin sentido.

    severity error y store_failures true: la corrida se detiene y las filas
    ofensoras quedan materializadas para poder mirarlas sin re-ejecutar nada.
*/

select
    belnr,
    lifnr,
    bldat,
    budat,
    datediff('day', budat, bldat) as days_before_document_date,
    _source_file,
    _extract_date

from {{ ref('stg_fbl1n__items') }}

where budat < bldat
