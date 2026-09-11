{{
    config(
        materialized = 'table',
        tags = ['marts', 'dim']
    )
}}

/*
    Calendario del proyecto. Grano: un dia.

    Generado con GENERATOR y no con dbt_utils.date_spine: el proyecto usa
    dbt_utils solo para tests declarados en YAML, y mantener los modelos libres
    de macros de paquetes deja el SQL linteable por sqlfluff con el templater
    jinja, sin necesidad de resolver dependencias en CI.

    El rango sale de vars del dbt_project.yml y cubre con margen las fechas de
    documento, vencimiento y compensacion del ledger.
*/

{% set spine_start = var('date_spine_start') %}
{% set spine_end = var('date_spine_end') %}

with spine as (

    -- Se generan 10 anos y se recorta por el extremo superior: mas simple y
    -- mas barato que calcular la cantidad exacta de filas en Jinja.
    select dateadd('day', seq4(), to_date('{{ spine_start }}')) as date_day
    from table(generator(rowcount => 3653))

),

bounded as (

    select date_day
    from spine
    where date_day <= to_date('{{ spine_end }}')

),

final as (

    select
        date_day,
        to_number(to_char(date_day, 'YYYYMMDD')) as date_key,

        year(date_day)                           as calendar_year,
        quarter(date_day)                        as calendar_quarter,
        month(date_day)                          as calendar_month,
        day(date_day)                            as calendar_day,
        weekofyear(date_day)                     as calendar_week,

        date_trunc('month', date_day)            as month_start_date,
        last_day(date_day, 'month')              as month_end_date,
        date_trunc('quarter', date_day)          as quarter_start_date,
        date_trunc('year', date_day)             as year_start_date,

        to_char(date_day, 'YYYY-MM')             as year_month,
        to_char(date_day, 'YYYY-"Q"Q')           as year_quarter,

        dayofweekiso(date_day)                   as day_of_week_iso,
        dayofweekiso(date_day) <= 5              as is_weekday,
        date_day = last_day(date_day, 'month')   as is_month_end,

        current_timestamp()                      as dbt_updated_at

    from bounded

)

select * from final
