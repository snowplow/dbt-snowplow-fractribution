{{ 
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  ) 
}}

-- Requires TrimLongPath UDF

WITH Conversions AS (
  SELECT DISTINCT customerId
  FROM {{ ref('snowplow_fractribution_conversions_by_customer_id') }}
),
NonConversions AS (
  SELECT
    customerId,
    MAX(visitStartTimestamp) AS nonConversionTimestamp
  FROM {{ ref('snowplow_fractribution_sessions_by_customer_id') }} se
  LEFT JOIN Conversions
    USING (customerId)
  WHERE Conversions.customerId IS NULL
  GROUP BY customerId
)
SELECT
  NonConversions.customerId,
  ARRAY_TO_STRING({{schema}}.TrimLongPath(
    ARRAY_AGG(channel) WITHIN GROUP (ORDER BY visitStartTimestamp), {{ var('path_lookback_steps') }}), ' > ') AS path,
  ARRAY_TO_STRING(
    {% for path_transform_name, _ in var('path_transforms')|reverse %}
      {{schema}}.{{path_transform_name}}(
    {% endfor %}
        ARRAY_AGG(channel) WITHIN GROUP (ORDER BY visitStartTimestamp)
    {% for _, arg_str in var('path_transforms') %}
      {% if arg_str %}, {{arg_str}}{% endif %})
    {% endfor %}
    , ' > ') AS transformedPath
FROM NonConversions
LEFT JOIN {{ ref('snowplow_fractribution_sessions_by_customer_id') }} se
  ON
    NonConversions.customerId = se.customerId
    AND DATEDIFF(day, visitStartTimestamp, nonConversionTimestamp)
      BETWEEN 0 AND {{ var('path_lookback_days') }}
GROUP BY NonConversions.customerId
