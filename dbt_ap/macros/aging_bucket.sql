{#
    Buckets de antiguedad de la deuda.

    `days_overdue` es la diferencia entre la fecha de corte del ledger y el
    vencimiento neto, y PUEDE SER NEGATIVA: el proceso de pagos admite pagos
    anticipados, asi que hay partidas compensadas antes de vencer y partidas
    abiertas que todavia no vencieron. Un bucket que arranque en 0 esconde ese
    tramo, que es justo donde se ve si el area paga antes de tiempo.
#}

{% macro aging_bucket(days_overdue) -%}
    case
        when {{ days_overdue }} is null then 'Sin vencimiento'
        when {{ days_overdue }} < 0     then 'No vencida'
        when {{ days_overdue }} = 0     then 'Vence hoy'
        when {{ days_overdue }} <= 30   then '1-30'
        when {{ days_overdue }} <= 60   then '31-60'
        when {{ days_overdue }} <= 90   then '61-90'
        else '90+'
    end
{%- endmacro %}


{% macro aging_bucket_sort(days_overdue) -%}
    {#- Orden explicito: los buckets son texto y no ordenan solos en el tablero. -#}
    case
        when {{ days_overdue }} is null then 0
        when {{ days_overdue }} < 0     then 1
        when {{ days_overdue }} = 0     then 2
        when {{ days_overdue }} <= 30   then 3
        when {{ days_overdue }} <= 60   then 4
        when {{ days_overdue }} <= 90   then 5
        else 6
    end
{%- endmacro %}
