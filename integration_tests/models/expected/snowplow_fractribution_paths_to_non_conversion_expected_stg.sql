select *

from {{ ref('snowplow_fractribution_paths_to_non_conversion_expected') }}
