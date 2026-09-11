{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'belnr',
        merge_exclude_columns = ['dbt_created_at'],
        on_schema_change = 'append_new_columns',
        cluster_by = ['is_open', 'netdt'],
        tags = ['marts', 'ap']
    )
}}

/*
    Estado actual del ledger de acreedores. Grano: una fila por BELNR.

    Por que la tabla guarda TODAS las partidas y no solo las abiertas
    -------------------------------------------------------------------
    El nombre dice open items porque es lo que se consume, pero la tabla
    materializa el ledger completo con un flag is_open. Si filtrara las
    compensadas, el dia que una partida se paga desapareceria del origen del
    merge y la fila abierta vieja se quedaria en la tabla para siempre: el merge
    actualiza y agrega, no borra lo que dejo de aparecer. Guardar las dos
    poblaciones y filtrar en consumo es lo que mantiene la tabla convergente.

    Por que el filtro incremental NO puede ser por BUDAT
    ----------------------------------------------------
    Seria lo natural: procesar solo lo contabilizado desde la ultima corrida.
    Rompe con los late-arriving documents. El feed trae todos los dias un ~2% de
    documentos con BUDAT de 15 a 45 dias atras, que recien ahora llegan al
    extracto. Un filtro por BUDAT los descartaria por viejos y nunca entrarian a
    la mart -- un agujero silencioso, del tipo que se descubre meses despues
    cuando alguien concilia contra SAP.

    _extract_date, en cambio, es cuando el pipeline VIO el dato, no cuando el
    hecho ocurrio. Es monotona por construccion y no la afecta la antiguedad
    contable del documento.

    Segunda condicion del filtro: `or is_open`
    -----------------------------------------
    El aging de una partida abierta cambia todos los dias aunque el documento no
    cambie -- la fecha de corte avanza. Sin re-mergear las abiertas, sus buckets
    quedarian congelados en el dia en que el documento aparecio por ultima vez.
    Es una poblacion chica (~3.600 filas) frente al historico acumulado, asi que
    el incremental sigue evitando la mayor parte del trabajo.
*/

with aging as (

    select * from {{ ref('int_ap_aging') }}

),

enriched as (

    select * from {{ ref('int_ap_items_enriched') }}

),

fx as (

    select * from {{ ref('int_ap_fx_revaluation') }}

),

final as (

    select
        -- --- claves ---
        enriched.belnr,
        enriched.kidno,
        enriched.lifnr,
        enriched.xblnr,
        enriched.augbl,

        -- --- acreedor ---
        enriched.vendor_name,
        enriched.country,
        enriched.purchasing_group,
        enriched.payment_terms,
        enriched.agreed_payment_days,

        -- --- documento ---
        enriched.blart,
        enriched.document_type_description,
        enriched.is_invoice,
        enriched.debit_credit_indicator,
        enriched.usnam,
        enriched.gkont,

        -- --- fechas ---
        enriched.bldat,
        enriched.budat,
        enriched.netdt,
        enriched.augdt,
        enriched.ledger_cutoff_date,

        -- --- importes ---
        -- wrbtr queda para trazar contra el documento original. Toda metrica
        -- agregada de este proyecto usa dmbtr: sumar wrbtr mezclaria monedas.
        enriched.wrbtr,
        enriched.waers,
        enriched.dmbtr,
        enriched.hwaer,
        enriched.fx_rate_at_document_date,

        -- --- estado y antiguedad ---
        enriched.is_open,
        aging.days_overdue,
        aging.days_outstanding,
        aging.credit_period_days,
        aging.aging_bucket,
        aging.aging_bucket_sort,
        aging.is_within_terms,
        enriched.verzn_report_unreliable,
        aging.verzn_drift_days,

        -- --- exposicion cambiaria (solo abiertas en moneda extranjera) ---
        fx.fx_rate_at_cutoff,
        fx.amount_local_at_cutoff,
        fx.fx_revaluation_local,
        fx.fx_result_local,
        fx.fx_drift_pct,

        -- --- linaje ---
        enriched._source_file,
        enriched._file_row_number,
        enriched._loaded_at,
        enriched._extract_date,

        -- dbt_created_at va en merge_exclude_columns: en un UPDATE del merge se
        -- deja intacto, para que siga marcando cuando la fila entro por primera
        -- vez a la mart. Sin esa exclusion, cada corrida lo pisaria y la
        -- columna terminaria siendo un duplicado de dbt_updated_at.
        current_timestamp() as dbt_created_at,
        current_timestamp() as dbt_updated_at

    from enriched

    inner join aging
        on enriched.belnr = aging.belnr

    left join fx
        on enriched.belnr = fx.belnr

)

select * from final

{% if is_incremental() %}

where
    _extract_date > (select max(_extract_date) from {{ this }})
    or is_open

{% endif %}
