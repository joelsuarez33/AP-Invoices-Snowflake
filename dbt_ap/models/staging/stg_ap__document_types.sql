{{
    config(
        materialized = 'view',
        tags = ['staging', 'maestros']
    )
}}

/*
    Clases de documento del ledger de acreedores y su indicador debe/haber
    esperado. stg_fbl1n__items deriva el indicador real del signo del importe;
    tener las dos vias permite testear que no se contradigan.
*/

with source as (

    select * from {{ ref('document_types') }}

),

renamed as (

    select
        trim(blart)                  as blart,
        trim(description)            as document_type_description,
        trim(debit_credit_indicator) as expected_debit_credit_indicator,
        is_invoice

    from source

)

select * from renamed
