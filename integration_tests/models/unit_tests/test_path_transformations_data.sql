{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

with data as (

  select 'Example > Video > Direct > Direct' as path

  union all

  select 'Direct > Direct' as path

  union all

  select 'a > a > a > Direct > a > Direct > Direct'

    union all

  select 'Direct'

  union all

  select '' as path

  union all

  select 'Example > Video > Direct' as path

  union all

  select 'Example > Video > ' as path

)

{% if target.type == 'redshift' %}

, final_form as (

  select
     path as transformed_path

  from data d
)

{% else %}

, final_form as (

  select
     {{ snowplow_utils.get_split_to_array('path', 'd', ' > ') }} as transformed_path

  from data d
)

{% endif %}

select

 *

from final_form
