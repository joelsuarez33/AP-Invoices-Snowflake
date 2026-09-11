{{
    config(
        materialized = 'table',
        tags = ['marts', 'ap'],
        cluster_by = ['clearing_month']
    )
}}

/*
    Comportamiento de pago: dias reales versus condicion pactada.
    Grano: una fila por acreedor y mes de compensacion.

    Dos varas distintas, y la diferencia entre ellas es el punto del modelo:

      - contra el vencimiento neto (NETDT): que dice el documento. Es la que
        mira Tesoreria, porque es la que genera intereses y reclamos.
      - contra la condicion pactada (ZTERM del maestro, en dias desde BLDAT):
        que se acordo con el proveedor. Es la que mira Compras.

    Las dos rara vez coinciden: el vencimiento que termina cargado en el
    documento se aparta del plazo maestro, y ese desvio -- terms_drift_days --
    es un hallazgo en si mismo.

    Tabla completa en cada corrida, no incremental: el universo de partidas
    compensadas se reescribe entero en segundos y una correccion de importe
    retroactiva tiene que poder recalcular meses ya cerrados.
*/

with aging as (

    select * from {{ ref('int_ap_aging') }}
    where not is_open

),

by_vendor_month as (

    select
        lifnr,
        date_trunc('month', augdt)                               as clearing_month,
        to_char(augdt, 'YYYY-MM')                                as clearing_year_month,

        count(*)                                                 as cleared_items,
        count(distinct augbl)                                    as payment_runs,

        -- Toda metrica agregada va en moneda local: sumar el importe en moneda
        -- de documento mezclaria pesos, dolares y euros en un solo numero.
        sum(dmbtr)                                               as cleared_amount_local,
        sum(abs(dmbtr))                                          as cleared_amount_local_abs,

        -- --- contra el vencimiento del documento ---
        avg(days_overdue)                                        as avg_days_vs_net_due,
        median(days_overdue)                                     as median_days_vs_net_due,
        max(days_overdue)                                        as max_days_vs_net_due,
        count_if(is_within_terms)                                as items_within_terms,

        -- --- contra la condicion pactada con el proveedor ---
        avg(datediff('day', bldat, augdt))                       as avg_days_to_pay,
        avg(datediff('day', bldat, augdt) - agreed_payment_days) as avg_days_vs_agreed_terms,
        avg(credit_period_days - agreed_payment_days)            as terms_drift_days,

        -- Ponderado por importe: un dia de atraso en una factura de 20 millones
        -- no pesa lo mismo que en una de 100 mil.
        div0(
            sum(datediff('day', bldat, augdt) * abs(dmbtr)),
            sum(abs(dmbtr))
        )                                                        as weighted_avg_days_to_pay,

        max(ledger_cutoff_date)                                  as ledger_cutoff_date

    from aging
    group by lifnr, date_trunc('month', augdt), to_char(augdt, 'YYYY-MM')

),

final as (

    select
        by_vendor_month.lifnr,
        dim_vendor.vendor_name,
        dim_vendor.country,
        dim_vendor.purchasing_group,
        dim_vendor.payment_terms,
        dim_vendor.agreed_payment_days,

        by_vendor_month.clearing_month,
        by_vendor_month.clearing_year_month,

        by_vendor_month.cleared_items,
        by_vendor_month.payment_runs,
        by_vendor_month.cleared_amount_local,
        by_vendor_month.cleared_amount_local_abs,

        round(by_vendor_month.avg_days_vs_net_due, 1)      as avg_days_vs_net_due,
        round(by_vendor_month.median_days_vs_net_due, 1)   as median_days_vs_net_due,
        by_vendor_month.max_days_vs_net_due,

        by_vendor_month.items_within_terms,
        round(
            100.0 * by_vendor_month.items_within_terms / by_vendor_month.cleared_items,
            1
        )                                                  as within_terms_pct,

        round(by_vendor_month.avg_days_to_pay, 1)          as avg_days_to_pay,
        round(by_vendor_month.weighted_avg_days_to_pay, 1) as weighted_avg_days_to_pay,
        round(by_vendor_month.avg_days_vs_agreed_terms, 1) as avg_days_vs_agreed_terms,
        round(by_vendor_month.terms_drift_days, 1)         as terms_drift_days,

        by_vendor_month.ledger_cutoff_date,
        current_timestamp()                                as dbt_updated_at

    from by_vendor_month

    inner join {{ ref('dim_vendor') }} as dim_vendor
        on by_vendor_month.lifnr = dim_vendor.lifnr

)

select * from final
