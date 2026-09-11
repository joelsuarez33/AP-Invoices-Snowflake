{{
    config(
        materialized = 'view',
        tags = ['staging', 'maestros']
    )
}}

/*
    Maestro de acreedores. La condicion de pago pactada (ZTERM) llega como
    codigo Zxxx; el numero de dias se extrae aca una sola vez para que
    fct_ap_payment_performance pueda comparar lo pagado contra lo pactado.
*/

with source as (

    select * from {{ ref('vendors') }}

),

renamed as (

    select
        trim(lifnr)                                              as lifnr,
        trim(vendor_name)                                        as vendor_name,
        trim(country)                                            as country,
        trim(payment_terms)                                      as payment_terms,
        trim(purchasing_group)                                   as purchasing_group,

        -- Z030 -> 30 dias. Si el codigo no sigue el patron, queda NULL y el
        -- indicador de cumplimiento no se calcula para ese acreedor, en vez de
        -- inventar un plazo por defecto.
        try_cast(substr(trim(payment_terms), 2) as number(4, 0)) as agreed_payment_days

    from source

)

select * from renamed
