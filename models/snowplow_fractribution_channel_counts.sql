{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

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
