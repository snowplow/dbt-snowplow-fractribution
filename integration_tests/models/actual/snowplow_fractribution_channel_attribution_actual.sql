{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% if target.type == 'snowflake'%}
select *

from {{ source('python_created_tables', 'snowplow_fractribution_channel_attribution') }}
-- Using source() here to avoid a node error when the table is not found in the models/ folder (as it is created by the python script, not dbt). 
{% else %}
-- non-Snowflake targets just need a dummy select as no tests will be run on these models
select 1 as col
{% endif %}