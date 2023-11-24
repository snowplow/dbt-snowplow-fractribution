{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

-- page view context is given as json string in csv. Parse json
with prep as (
select
  *
   except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0),
   JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) AS contexts_com_snowplowanalytics_snowplow_web_page_1_0_0

from {{ ref('snowplow_fractribution_events') }}
)

select
  *
  except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0),
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.id') as id
    from unnest(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_snowplow_web_page_1_0_0

from prep
