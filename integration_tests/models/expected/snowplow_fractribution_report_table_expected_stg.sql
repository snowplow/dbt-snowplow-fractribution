select *

from {{ ref('snowplow_fractribution_report_table_expected') }}
where attribution_type = '{{ var("snowplow__attribution_model_for_snowpark", "NULL") }}'