{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

SELECT
  con.customer_id,
  conversion_tstamp,
  revenue,
  ARRAY_TO_STRING({{schema}}.TrimLongPath(
    ARRAY_AGG(channel) WITHIN GROUP (ORDER BY visit_start_tstamp), {{ var('path_lookback_steps') }}),
    ' > ') AS path,
  ARRAY_TO_STRING(
    {% for path_transform_name, _ in var('path_transforms')|reverse %}
      {{schema}}.{{path_transform_name}}(
    {% endfor %}
        ARRAY_AGG(channel) WITHIN GROUP (ORDER BY visit_start_tstamp)
    {% for _, arg_str in var('path_transforms') %}
      {% if arg_str %}, {{arg_str}}{% endif %})
    {% endfor %}
    , ' > ') AS transformed_path
FROM {{ ref('snowplow_fractribution_conversions_by_customer_id') }} con
LEFT JOIN {{ ref('snowplow_fractribution_sessions_by_customer_id') }} se
  ON
    con.customer_id = se.customer_id
    AND DATEDIFF(day, visit_start_tstamp, conversion_tstamp)
      BETWEEN 0 AND {{ var('path_lookback_days') }}
GROUP BY
  con.customer_id,
  conversion_tstamp,
  revenue
