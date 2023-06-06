-- page view context is given as json string in csv. Parse json
with prep as (
select
  *
from {{ ref('snowplow_fractribution_events') }}
)


select
 *
from prep
