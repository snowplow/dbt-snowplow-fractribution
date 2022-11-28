{% macro channel_spend() %}
    {{ return(adapter.dispatch('channel_spend', 'snowplow_fractribution')()) }}
{% endmacro %}


{% macro default__channel_spend() %}
    WITH channels AS (
        SELECT ARRAY_AGG(DISTINCT channel) AS c FROM {{ ref('snowplow_fractribution_channel_counts') }}
    )
    SELECT
    CAST(channel.value AS STRING) AS channel,
    10000 AS spend
    FROM
    channels,
    LATERAL FLATTEN(c) channel
{% endmacro %}


--add a comment here that shows the table output - 2 cols, one for spend, one for channel_name

