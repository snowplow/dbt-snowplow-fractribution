
select *

from {{ ref('snowplow_fractribution_path_summary_with_channels_expected') }}
where attribution_type = '{{ var("snowplow__attribution_model_for_snowpark", "NULL") }}'
