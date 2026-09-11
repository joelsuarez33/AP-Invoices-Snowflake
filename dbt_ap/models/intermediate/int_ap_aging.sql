{{
    config(
        materialized = 'view',
        tags = ['intermediate']
    )
}}

/*
    Recalcula la antiguedad de cada partida.

    No se usa VERZN. El report lo calcula contra la fecha en que se ejecuto, y
    el mismo documento trae un valor distinto en cada extracto: es un artefacto
    de presentacion, no un hecho. Aca la referencia es la fecha de corte del
    ledger (max _extract_date), que hace el resultado reproducible.

    Los dias pueden ser negativos y eso no es un caso borde: el proceso de pagos
    compensa partidas antes del vencimiento, y hay partidas abiertas que
    todavia no vencieron. Un bucket que arranque en cero borra justamente el
    tramo donde se ve si el area esta pagando antes de tiempo.

    Para una partida compensada la referencia es AUGDT: mide la demora con la
    que efectivamente se pago. Para una abierta es la fecha de corte: mide la
    demora acumulada hasta hoy.
*/

with enriched as (

    select * from {{ ref('int_ap_items_enriched') }}

),

aged as (

    select
        belnr,
        lifnr,
        vendor_name,
        blart,
        waers,
        wrbtr,
        dmbtr,
        hwaer,
        bldat,
        budat,
        netdt,
        augdt,
        augbl,
        is_open,
        agreed_payment_days,
        ledger_cutoff_date,
        _extract_date,

        -- Fecha contra la que se mide: compensacion si la hay, corte si no.
        coalesce(augdt, ledger_cutoff_date)                         as as_of_date,

        datediff('day', netdt, coalesce(augdt, ledger_cutoff_date)) as days_overdue,
        datediff('day', budat, coalesce(augdt, ledger_cutoff_date)) as days_outstanding,
        datediff('day', bldat, netdt)                               as credit_period_days,

        -- Diferencia entre el valor que imprimio el report y el recalculado.
        -- Distinto de cero es lo normal: el report se ejecuto otro dia. Sirve
        -- para explicar la brecha cuando alguien compara contra la pantalla.
        case
            when is_open
                then datediff('day', netdt, ledger_cutoff_date) - verzn_report_unreliable
        end                                                         as verzn_drift_days

    from enriched

),

bucketed as (

    select
        *,
        {{ aging_bucket('days_overdue') }}      as aging_bucket,
        {{ aging_bucket_sort('days_overdue') }} as aging_bucket_sort,

        -- Se pago o se paga dentro del vencimiento neto.
        days_overdue <= 0                       as is_within_terms

    from aged

)

select * from bucketed
