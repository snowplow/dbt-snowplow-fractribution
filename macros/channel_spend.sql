-- User supplied SQL script to extract total ad spend by channel, by replacing the SQL query in the default__channel_spend() macro.
-- Required output schema:
--  channel: STRING NOT NULL
--  spend: FLOAT64 (Use the same monetary units as conversion revenue, and NULL if unknown.)
--
-- Note that all flags are passed into this template (e.g. conversion_window_start/end_date).

-- Example (simplified) query:
-- SELECT channel, 
--        SUM(spend_usd) AS spend 
-- FROM example_spend_table 
-- GROUP BY 1

-- Example table output for the user-supplied SQL:
--   Channel     |  Spend
--  ------------------------
--   direct      |  1050.02
--   paid_search |  10490.11 
--   etc...


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


