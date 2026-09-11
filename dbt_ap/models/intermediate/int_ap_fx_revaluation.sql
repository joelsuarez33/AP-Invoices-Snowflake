{{
    config(
        materialized = 'view',
        tags = ['intermediate', 'fx']
    )
}}

/*
    Exposicion cambiaria de las partidas abiertas en moneda extranjera.

    En un ledger que cierra en ARS esta es la metrica mas cara del dataset. Una
    factura en dolares se contabilizo con el tipo de cambio del dia del
    documento y eso quedo fijo en DMBTR. Pero la obligacion sigue siendo en
    dolares: lo que hay que pagar hoy, medido en pesos, es WRBTR a la
    cotizacion de hoy. La diferencia entre las dos cifras no aparece en ningun
    lado del extracto y sin embargo es plata.

    Convencion de signo: los importes de factura son negativos (acreditan al
    proveedor). fx_revaluation_local negativo = el pasivo en moneda local
    crecio = perdida por diferencia de cambio. Se expone tambien
    fx_result_local con el signo invertido, que es como lo lee un resultado.

    Solo partidas abiertas: una partida compensada ya se pago al tipo de cambio
    que fuera, y su diferencia de cambio es un hecho consumado, no una
    exposicion.
*/

with aged as (

    select * from {{ ref('int_ap_items_enriched') }}

),

fx_rates as (

    select * from {{ ref('stg_ap__fx_rates') }}

),

cutoff as (

    select max(ledger_cutoff_date) as ledger_cutoff_date
    from aged

),

rate_at_cutoff as (

    -- Ultima cotizacion disponible en o antes de la fecha de corte. Que la
    -- serie no tenga un fin de semana o un feriado no puede dejar la
    -- exposicion en NULL.
    select
        fx_rates.currency,
        fx_rates.rate_to_local as fx_rate_at_cutoff,
        fx_rates.rate_date     as fx_rate_at_cutoff_date
    from fx_rates
    inner join cutoff
        on fx_rates.rate_date <= cutoff.ledger_cutoff_date
    qualify row_number() over (
            partition by fx_rates.currency
            order by fx_rates.rate_date desc
        ) = 1

),

exposure as (

    select
        aged.belnr,
        aged.lifnr,
        aged.vendor_name,
        aged.country,
        aged.blart,
        aged.bldat,
        aged.budat,
        aged.netdt,
        aged.ledger_cutoff_date,

        aged.waers,
        aged.wrbtr,
        aged.hwaer,

        aged.fx_rate_at_document_date,
        rate_at_cutoff.fx_rate_at_cutoff,
        rate_at_cutoff.fx_rate_at_cutoff_date,

        -- Lo que quedo contabilizado: WRBTR al tipo de cambio del documento.
        aged.dmbtr                                           as amount_local_at_document_date,

        -- Lo que representa hoy la misma obligacion en moneda extranjera.
        cast(
            aged.wrbtr * rate_at_cutoff.fx_rate_at_cutoff as number(19, 2)
        )                                                    as amount_local_at_cutoff,

        -- Negativo = el pasivo en moneda local crecio.
        cast(
            aged.wrbtr * rate_at_cutoff.fx_rate_at_cutoff - aged.dmbtr as number(19, 2)
        )                                                    as fx_revaluation_local,

        -- Mismo numero con signo de resultado: positivo = ganancia.
        cast(
            aged.dmbtr - aged.wrbtr * rate_at_cutoff.fx_rate_at_cutoff as number(19, 2)
        )                                                    as fx_result_local,

        -- Variacion relativa de la cotizacion desde que se contabilizo.
        round(
            (rate_at_cutoff.fx_rate_at_cutoff / nullif(aged.fx_rate_at_document_date, 0) - 1) * 100,
            2
        )                                                    as fx_drift_pct,

        datediff('day', aged.bldat, aged.ledger_cutoff_date) as days_exposed,

        aged._extract_date

    from aged

    inner join rate_at_cutoff
        on aged.waers = rate_at_cutoff.currency

    where aged.is_open
        -- La moneda local contra si misma no tiene exposicion: la cotizacion es 1
        -- por definicion y las filas solo agregarian ruido al tablero.
        and aged.waers != aged.hwaer

)

select * from exposure
