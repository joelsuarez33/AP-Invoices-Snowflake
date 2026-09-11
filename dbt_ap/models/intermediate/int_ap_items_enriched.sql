{{
    config(
        materialized = 'view',
        tags = ['intermediate']
    )
}}

/*
    Une las partidas con los tres maestros y fija la fecha de corte del ledger.

    La fecha de corte es max(_extract_date) sobre el propio ledger, no
    CURRENT_DATE. Consecuencia practica: correr el pipeline hoy sobre los datos
    de ayer devuelve exactamente lo que devolvio ayer. Con CURRENT_DATE, cada
    re-ejecucion produciria un aging distinto y ningun numero seria reproducible.

    El tipo de cambio que se pega aca es el de la fecha del documento: es el que
    uso el origen para calcular DMBTR. La conversion a fecha de corte -- que es
    otra cosa, y es la exposicion cambiaria -- vive en int_ap_fx_revaluation.
*/

with items as (

    select * from {{ ref('stg_fbl1n__items') }}

),

vendors as (

    select * from {{ ref('stg_ap__vendors') }}

),

document_types as (

    select * from {{ ref('stg_ap__document_types') }}

),

fx_rates as (

    select * from {{ ref('stg_ap__fx_rates') }}

),

ledger_cutoff as (

    -- Un solo valor para todo el modelo: el ultimo extracto ingestado.
    select max(_extract_date) as ledger_cutoff_date
    from items

),

joined as (

    select
        -- --- claves ---
        items.belnr,
        items.kidno,
        items.lifnr,
        items.xblnr,
        items.augbl,

        -- --- acreedor ---
        vendors.vendor_name,
        vendors.country,
        vendors.purchasing_group,
        vendors.payment_terms,
        vendors.agreed_payment_days,

        -- --- clase de documento ---
        items.blart,
        document_types.document_type_description,
        document_types.is_invoice,
        items.debit_credit_indicator,
        document_types.expected_debit_credit_indicator,

        -- --- importes ---
        items.wrbtr,
        items.waers,
        items.dmbtr,
        items.hwaer,
        -- Cotizacion vigente a la fecha del documento. Es la que explica la
        -- relacion entre WRBTR y DMBTR en el origen.
        fx_rates.rate_to_local as fx_rate_at_document_date,

        -- --- fechas ---
        items.bldat,
        items.budat,
        items.netdt,
        items.augdt,
        ledger_cutoff.ledger_cutoff_date,

        -- --- estado ---
        items.is_open,
        items.verzn_report_unreliable,
        items.usnam,
        items.gkont,

        -- --- metadata de carga ---
        items._source_file,
        items._file_row_number,
        items._loaded_at,
        items._extract_date

    from items

    -- inner join: un acreedor desconocido no puede aparecer en la mart en
    -- silencio. El test de relationships sobre stg_fbl1n__items.lifnr falla
    -- primero y deja claro que hay que actualizar el maestro.
    inner join vendors
        on items.lifnr = vendors.lifnr

    inner join document_types
        on items.blart = document_types.blart

    left join fx_rates
        on items.waers = fx_rates.currency
            and items.bldat = fx_rates.rate_date

    cross join ledger_cutoff

)

select * from joined
