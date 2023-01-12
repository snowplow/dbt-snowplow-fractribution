{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with paths_to_conversion as (

  select
    transformed_path,
    count(*) as conversions,
    sum(revenue) as revenue

  from {{ ref('snowplow_fractribution_paths_to_conversion') }}

  group by 1

)

, paths_to_non_conversion as (

  select
    transformed_path,
    count(*) as non_conversions

  from {{ ref('snowplow_fractribution_paths_to_non_conversion') }}

  group by 1
)

select
  coalesce(c.transformed_path, n.transformed_path) as transformed_path,
  coalesce(c.conversions, 0) as conversions,
  coalesce(n.non_conversions, 0) as non_conversions,
  c.revenue

from paths_to_conversion c

full join paths_to_non_conversion n
  on c.transformed_path = n.transformed_path
