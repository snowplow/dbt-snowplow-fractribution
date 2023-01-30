select
  customer_id,
  cast(conversion_tstamp as {{ dbt.type_timestamp() }}) as conversion_tstamp,
  revenue

from {{ ref('snowplow_fractribution_conversions_by_customer_id_expected') }}
