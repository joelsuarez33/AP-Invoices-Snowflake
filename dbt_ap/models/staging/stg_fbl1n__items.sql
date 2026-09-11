{{
    config(
        materialized = 'view',
        tags = ['staging', 'fbl1n']
    )
}}

/*
    Unica capa donde se castea. RAW es texto crudo; de aca para abajo los tipos
    son los que dicen las columnas.

    Y es tambien donde el change feed se resuelve a estado actual: el QUALIFY
    del final se queda con la ultima version de cada BELNR. RAW conserva el
    historial completo -- la version original de un documento, la correccion de
    importe, la compensacion -- y esta vista lo colapsa. Esa es la razon por la
    que el pipeline no necesita un snapshot completo del ledger todos los dias.

    Desempate: _extract_date primero, _file_row_number despues. El segundo
    criterio solo actua si un mismo BELNR apareciera dos veces en un mismo
    archivo; el generador garantiza que no pasa, pero un extracto real no
    ofrece esa garantia y la resolucion no puede quedar indefinida.
*/

with source as (

    select * from {{ source('sap_fi', 'fbl1n_items') }}

),

casted as (

    select
        -- --- claves ---
        {{ sap_to_text('belnr') }}       as belnr,
        {{ sap_to_text('kidno') }}       as kidno,
        {{ sap_to_text('lifnr') }}       as lifnr,
        {{ sap_to_text('xblnr') }}       as xblnr,

        -- --- atributos ---
        {{ sap_to_text('blart') }}       as blart,
        {{ sap_to_text('usnam') }}       as usnam,
        {{ sap_to_text('gkont') }}       as gkont,
        {{ sap_to_text('augbl') }}       as augbl,

        -- --- importes ---
        -- wrbtr esta en la moneda del documento: NO es agregable entre monedas.
        -- Toda metrica agregada del proyecto va sobre dmbtr.
        {{ sap_to_amount('wrbtr') }}     as wrbtr,
        {{ sap_to_text('waers') }}       as waers,
        {{ sap_to_amount('dmbtr') }}     as dmbtr,
        {{ sap_to_text('hwaer') }}       as hwaer,

        -- --- fechas ---
        {{ sap_to_date('bldat') }}       as bldat,
        {{ sap_to_date('budat') }}       as budat,
        {{ sap_to_date('netdt') }}       as netdt,
        {{ sap_to_date('augdt') }}       as augdt,

        -- --- campo de origen no confiable ---
        -- VERZN lo calcula el report en el momento de ejecutarse, contra la
        -- fecha de ejecucion. No es un hecho del documento: el mismo documento
        -- trae un VERZN distinto en cada extracto. Se conserva para poder
        -- reconciliar contra la pantalla de SAP, pero el aging se recalcula en
        -- int_ap_aging. No usar esta columna para medir.
        {{ sap_to_integer('verzn') }}    as verzn_report_unreliable,

        -- --- columnas que la variante de layout trae sin poblar ---
        {{ sap_to_text('icon_status') }} as icon_status,
        {{ sap_to_text('xref1') }}       as xref1,
        {{ sap_to_text('zlsch') }}       as zlsch,
        {{ sap_to_text('zlspr') }}       as zlspr,

        -- --- metadata de carga ---
        _source_file,
        _file_row_number,
        _loaded_at,
        _extract_date

    from source

),

derived as (

    select
        *,

        -- Una partida esta abierta mientras no tenga fecha de compensacion.
        augdt is null as is_open,

        -- Indicador debe/haber de SAP derivado del signo del importe local:
        -- H (Haben) acredita al proveedor -- facturas, importe negativo.
        -- S (Soll)  debita  al proveedor -- notas de credito y pagos.
        case
            when dmbtr < 0 then 'H'
            else 'S'
        end           as debit_credit_indicator

    from casted

)

select * from derived

-- Ultima version gana. Es la misma regla que aplica el generador al
-- reconstruir su universo de documentos: si las dos difirieran, el pipeline
-- no seria verificable contra su origen.
qualify row_number() over (
        partition by belnr
        order by _extract_date desc, _file_row_number desc
    ) = 1
