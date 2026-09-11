{{
    config(
        materialized = 'table',
        tags = ['marts', 'dim']
    )
}}

/*
    Dimension de acreedores. Grano: una fila por LIFNR.

    Atributos del maestro mas tres metricas de actividad que son atributos de la
    relacion, no del hecho: cuando aparecio el acreedor por primera vez, cuando
    por ultima, y cuantos documentos acumula. Sirven para filtrar proveedores
    inactivos en el tablero sin tener que golpear la tabla de hechos.

    Los importes NO viven aca: los agregados van en la fact.
*/

with vendors as (

    select * from {{ ref('stg_ap__vendors') }}

),

activity as (

    select
        lifnr,
        min(bldat)              as first_document_date,
        max(bldat)              as last_document_date,
        count(*)                as documents_total,
        count_if(is_open)       as open_items_total,
        max(ledger_cutoff_date) as ledger_cutoff_date
    from {{ ref('int_ap_items_enriched') }}
    group by lifnr

),

final as (

    select
        vendors.lifnr,
        vendors.vendor_name,
        vendors.country,
        vendors.payment_terms,
        vendors.agreed_payment_days,
        vendors.purchasing_group,

        activity.first_document_date,
        activity.last_document_date,
        coalesce(activity.documents_total, 0)  as documents_total,
        coalesce(activity.open_items_total, 0) as open_items_total,

        -- Un acreedor sin partidas abiertas y sin documentos nuevos en 90 dias
        -- no aparece en la operacion corriente. Un acreedor que nunca tuvo
        -- documentos tampoco: por eso el coalesce y no un OR a secas, que
        -- devolveria NULL cuando no hay actividad.
        coalesce(
            activity.open_items_total > 0
            or datediff('day', activity.last_document_date, activity.ledger_cutoff_date) <= 90,
            false
        )                                      as is_active,

        current_timestamp()                    as dbt_updated_at

    from vendors

    -- left join: un acreedor del maestro sin ningun documento sigue siendo una
    -- fila valida de la dimension.
    left join activity
        on vendors.lifnr = activity.lifnr

)

select * from final
