{#
    Casteos del formato de display de SAP.

    RAW guarda todo como texto porque una carga no puede fallar por un dato con
    formato raro: tiene que aterrizar y quedar visible. El costo de esa decision
    es que el casteo se repite en staging, y por eso vive en macros -- el
    formato de fecha del report aparece una sola vez en todo el proyecto.
#}

{% macro sap_to_date(column) -%}
    {#- El report exporta las fechas como DD.MM.YYYY. -#}
    to_date(nullif(trim({{ column }}), ''), 'DD.MM.YYYY')
{%- endmacro %}


{% macro sap_to_amount(column) -%}
    {#- Importes con punto decimal y signo adelante. NUMBER(19,2) cubre el
        rango del ledger en ARS sin perder centavos. -#}
    cast(nullif(trim({{ column }}), '') as number(19, 2))
{%- endmacro %}


{% macro sap_to_integer(column) -%}
    cast(nullif(trim({{ column }}), '') as number(10, 0))
{%- endmacro %}


{% macro sap_to_text(column) -%}
    {#- Vacio y NULL son lo mismo aguas abajo: una columna sin poblar. -#}
    nullif(trim({{ column }}), '')
{%- endmacro %}
