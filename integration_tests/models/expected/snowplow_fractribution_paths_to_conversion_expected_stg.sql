select
  customer_id,
  cast(conversion_tstamp as {{ dbt.type_timestamp() }}) as conversion_tstamp,
  revenue,
  path,
  transformed_path

from {{ ref('snowplow_fractribution_paths_to_conversion_expected') }}
