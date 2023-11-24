{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}


select *

from {{ ref('snowplow_fractribution_path_summary_with_channels_expected') }}
where attribution_type = '{{ var("snowplow__attribution_model_for_snowpark", "NULL") }}'
