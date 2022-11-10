-- enter a single column (or calculated value) for the value assigned
-- to a given conversion event. This column should be a float / double.
-- by default we use tr_total but this may be something like
-- com_acme_checkout_1[SAFE_OFFSET(0)].cart_value
-- do not alias this value
{% macro conversion_value() %}
    {{ return(adapter.dispatch('conversion_value', 'fractribution')()) }}
{% endmacro %}

{% macro default__conversion_value() %}
    tr_total
{% endmacro %}