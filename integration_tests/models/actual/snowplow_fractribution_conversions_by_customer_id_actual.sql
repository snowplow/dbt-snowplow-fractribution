select *

from {{ ref('snowplow_fractribution_conversions_by_customer_id') }}
