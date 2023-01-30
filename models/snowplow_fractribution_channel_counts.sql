{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select
  channel,
  campaign,
  source,
  medium,
  count(*) as number_of_sessions

from {{ ref('snowplow_fractribution_sessions_by_customer_id') }}

group by 1,2,3,4

order by channel, number_of_sessions desc
