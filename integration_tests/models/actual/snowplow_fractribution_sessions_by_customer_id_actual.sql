select *

from {{ ref('snowplow_fractribution_sessions_by_customer_id') }}
