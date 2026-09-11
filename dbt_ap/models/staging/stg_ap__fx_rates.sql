{{
    config(
        materialized = 'view',
        tags = ['staging', 'maestros']
    )
}}

/*
    Serie de tipos de cambio diarios contra la moneda local del ledger.
    Es la unica fuente de conversion del proyecto: ni el generador ni los
    modelos derivan una cotizacion por su cuenta.
*/

with source as (

    select * from {{ ref('fx_rates') }}

),

renamed as (

    select
        rate_date,
        trim(currency)                     as currency,
        cast(rate_to_ars as number(18, 6)) as rate_to_local

    from source

)

select * from renamed
