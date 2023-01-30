{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

-- Requires macro trim_long_path

with string_aggs as (

  select distinct
    c.customer_id,
    c.conversion_tstamp,
    revenue,
    {{ snowplow_utils.get_string_agg('channel', 's', separator=' > ', sort_numeric=false, order_by_column='visit_start_tstamp', partition_by_columns='c.customer_id', order_by_column_prefix='s') }} as path,
    {{ snowplow_utils.get_string_agg('channel', 's', separator=' > ', sort_numeric=false, order_by_column='visit_start_tstamp', partition_by_columns='c.customer_id', order_by_column_prefix='s') }} as transformed_path

  from {{ ref('snowplow_fractribution_conversions_by_customer_id') }} c

  left join {{ ref('snowplow_fractribution_sessions_by_customer_id') }} s
  on c.customer_id = s.customer_id
    and {{ datediff('s.visit_start_tstamp', 'c.conversion_tstamp', 'day') }}  >= 0
    and {{ datediff('s.visit_start_tstamp', 'c.conversion_tstamp', 'day') }} <= {{ var('path_lookback_days') }}

{% if target.type in ['snowflake', 'bigquery'] -%}
  group by 1,2,3
{%- endif %}

)

, arrays as (

  select
    customer_id,
    conversion_tstamp,
    revenue,
    {{ snowplow_utils.get_split_to_array('path', 's', ' > ') }} as path,
    {{ snowplow_utils.get_split_to_array('transformed_path', 's', ' > ') }} as transformed_path

  from string_aggs s

)

{{ transform_paths('conversions') }}

select
  customer_id,
  conversion_tstamp,
  revenue,
  {{ snowplow_utils.get_array_to_string('path', 'p', ' > ') }} as path,
  {{ snowplow_utils.get_array_to_string('transformed_path', 'p', ' > ') }} as transformed_path

from path_transforms p
