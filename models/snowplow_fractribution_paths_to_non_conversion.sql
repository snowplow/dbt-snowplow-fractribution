{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

-- Requires macro trim_long_path


with conversions as (

  select distinct customer_id

  from {{ ref('snowplow_fractribution_conversions_by_customer_id') }}
)

, non_conversions as (

  select
    s.customer_id,
    max(s.visit_start_tstamp) as non_conversion_tstamp

  from {{ ref('snowplow_fractribution_sessions_by_customer_id') }} s

  left join conversions c
    on s.customer_id = c.customer_id

  where c.customer_id is null

  group by 1
)

, string_aggs as (

  select distinct
    n.customer_id,
    {{ snowplow_utils.get_string_agg('channel', 's', separator=' > ', order_by_column='visit_start_tstamp', sort_numeric=false, partition_by_columns='n.customer_id', order_by_column_prefix='s') }} as path,
    {{ snowplow_utils.get_string_agg('channel', 's', separator=' > ', order_by_column='visit_start_tstamp', sort_numeric=false, partition_by_columns='n.customer_id', order_by_column_prefix='s') }} as transformed_path


  from non_conversions n

  left join {{ ref('snowplow_fractribution_sessions_by_customer_id') }} s
  on n.customer_id = s.customer_id
    and {{ datediff('s.visit_start_tstamp', 'n.non_conversion_tstamp', 'day') }}  >= 0
    and {{ datediff('s.visit_start_tstamp', 'n.non_conversion_tstamp', 'day') }} <= {{ var('path_lookback_days') }}

{% if target.type in ['snowflake', 'bigquery'] -%}
  group by 1
{%- endif %}

)

, arrays as (

    select
      customer_id,
      {{ snowplow_utils.get_split_to_array('path', 's', ' > ') }} as path,
      {{ snowplow_utils.get_split_to_array('transformed_path', 's', ' > ') }} as transformed_path

    from string_aggs s

)

{{ transform_paths('non_conversions', 'arrays') }}

select
  customer_id,
  {{ snowplow_utils.get_array_to_string('path', 'p', ' > ') }} as path,
  {{ snowplow_utils.get_array_to_string('transformed_path', 'p', ' > ') }} as transformed_path

from path_transforms p
