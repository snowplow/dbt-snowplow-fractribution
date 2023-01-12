select *

from {{ ref('snowplow_fractribution_path_summary_expected') }}
