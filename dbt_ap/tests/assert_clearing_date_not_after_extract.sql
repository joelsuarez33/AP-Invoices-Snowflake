{{
    config(
        severity = 'error',
        store_failures = true,
        alias = 'fail_augdt_after_extract',
        tags = ['invariantes', 'fbl1n']
    )
}}

/*
    Invariante: AUGDT nunca es posterior al _EXTRACT_DATE de la fila que lo trajo.

    Una compensacion no puede aparecer en un extracto anterior a la fecha en que
    ocurrio. Si pasa, hay una de dos cosas:

      - el nombre del archivo miente sobre su contenido, y entonces
        _extract_date -- que es la columna que gobierna el filtro incremental y
        la resolucion de versiones -- no es confiable;
      - alguien cargo un archivo fuera de orden y el QUALIFY eligio como
        vigente una version que no lo es.

    Los dos casos corrompen el estado actual del ledger de forma silenciosa.
    Este test cierra la unica puerta por la que eso puede entrar.
*/

select
    belnr,
    lifnr,
    augdt,
    augbl,
    _extract_date,
    datediff('day', _extract_date, augdt) as days_ahead_of_extract,
    _source_file

from {{ ref('stg_fbl1n__items') }}

where augdt > _extract_date
