
select *

from {{ ref('snowplow_fractribution_channel_attribution_expected') }}
where attribution_type = '{{ var("snowplow__attribution_model_for_snowpark", "NULL") }}'
