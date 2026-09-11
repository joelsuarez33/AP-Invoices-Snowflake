{#
    Los esquemas custom se usan literalmente, sin prefijar con el schema del
    target. Por defecto dbt genera ANALYTICS_STAGING, ANALYTICS_MARTS, etc.;
    aca queremos STAGING y MARTS a secas, que son los esquemas que crea
    sql/00_setup_account.sql y sobre los que estan otorgados los grants.

    Cuando un modelo no declara +schema, cae en el schema del target.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
