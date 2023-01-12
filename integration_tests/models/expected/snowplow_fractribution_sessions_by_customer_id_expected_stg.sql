select
  customer_id,
  cast(visit_start_tstamp as {{ dbt.type_timestamp() }})  as visit_start_tstamp,
  channel,
  referral_path,
  campaign,
  source,
  medium

  from {{ ref('snowplow_fractribution_sessions_by_customer_id_expected') }}
