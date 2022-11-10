{% macro channel_spend() %}
    {{ return(adapter.dispatch('channel_spend', 'fractribution')()) }}
{% endmacro %}


{% macro default__channel_spend() %}
    WITH channels AS (
        SELECT ARRAY_AGG(DISTINCT channel) AS c FROM {{ ref('channel_counts') }}
    )
    SELECT
    CAST(channel.value AS STRING) AS channel,
    10000 AS spend
    FROM
    channels,
    LATERAL FLATTEN(c) channel
{% endmacro %}