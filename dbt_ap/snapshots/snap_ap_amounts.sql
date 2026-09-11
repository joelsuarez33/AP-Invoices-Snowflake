{#
    Unico snapshot del proyecto.

    Que captura: las correcciones de importe. El change feed re-emite un
    documento existente con WRBTR/DMBTR distintos, y el QUALIFY de staging deja
    ver solo la ultima version. RAW conserva el historial, pero consultarlo
    exige reconstruir a mano la vigencia de cada version. Este snapshot lo
    expone como SCD2, que es la forma en que un contador quiere leerlo: "este
    documento valia X entre tal y tal fecha".

    Por que solo uno:
      - Las compensaciones NO necesitan snapshot. AUGDT ya es la fecha del
        hecho: el documento trae consigo cuando se compenso, sin depender de
        cuando lo vio el pipeline.
      - El estado abierta/compensada tampoco: se deriva de AUGDT.
      - Un snapshot completo del ledger seria redundante con RAW, que ya es
        append-only y guarda cada version con su _extract_date.

    strategy='check' y no 'timestamp': el origen no tiene una columna de
    modificacion confiable. VERZN cambia en cada corrida sin que el documento
    cambie, asi que usarlo como marca de tiempo generaria una version nueva por
    dia para cada partida abierta.
#}

{% snapshot snap_ap_amounts %}

{{
    config(
        target_schema = 'SNAPSHOTS',
        unique_key = 'belnr',
        strategy = 'check',
        check_cols = ['wrbtr', 'dmbtr'],
        invalidate_hard_deletes = false
    )
}}

select
    belnr,
    kidno,
    lifnr,
    blart,
    waers,
    wrbtr,
    hwaer,
    dmbtr,
    bldat,
    budat,
    netdt,
    _extract_date as source_extract_date

from {{ ref('stg_fbl1n__items') }}

{% endsnapshot %}
