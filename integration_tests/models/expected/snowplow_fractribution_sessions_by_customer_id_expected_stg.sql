-- other expected files with timestamps were easier to be changed manually in the csvs

with prep as (
  select
    customer_id,
    cast(visit_start_tstamp as {{ dbt.type_timestamp() }})  as visit_start_tstamp,
    channel,
    referral_path,
    campaign,
    source,
    medium

  from {{ ref('snowplow_fractribution_sessions_by_customer_id_expected') }}
)

select
  customer_id,
  {{ dateadd('hour', '1', 'visit_start_tstamp') }} as visit_start_tstamp,
    channel,
  referral_path,
  campaign,
  source,
  medium

from prep
