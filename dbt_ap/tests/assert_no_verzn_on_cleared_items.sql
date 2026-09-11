{{
    config(
        severity = 'error',
        store_failures = true,
        alias = 'fail_verzn_on_cleared',
        tags = ['invariantes', 'fbl1n']
    )
}}

/*
    Invariante: VERZN esta poblado si y solo si la partida esta abierta.

    La demora tras vencimiento neto es un dato de partidas pendientes: una vez
    compensado el documento, el report deja la columna vacia. Encontrar las dos
    cosas juntas -- AUGDT poblado y VERZN tambien -- significa que la fila mezcla
    dos versiones distintas del documento, y eso invalida la premisa sobre la
    que el QUALIFY de staging resuelve el change feed.

    Este test es el guardian de esa premisa. Si cae, hay que revisar la
    resolucion de versiones antes de mirar cualquier metrica.
*/

select
    belnr,
    lifnr,
    augdt,
    augbl,
    verzn_report_unreliable,
    _source_file,
    _file_row_number,
    _extract_date

from {{ ref('stg_fbl1n__items') }}

where augdt is not null
    and verzn_report_unreliable is not null
