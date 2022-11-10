-- Define a conditional (where clause) that filters to conversion events
-- by default we use tr_total but this may refer to an entity
-- or event (e.g., event_name = 'checkout')
{% macro conversion_clause() %}
    {{ return(adapter.dispatch('conversion_clause', 'fractribution')()) }}
{% endmacro %}

{% macro default__conversion_clause() %}
    tr_total > 0
{% endmacro %}