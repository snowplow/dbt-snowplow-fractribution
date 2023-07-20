{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select
  case when events.user_id is not null and events.user_id != '' then 'u' || events.user_id -- use event user_id
    {% if var('snowplow__use_snowplow_web_user_mapping_table') %}
       when user_mapping.domain_userid is not null then 'u' || user_mapping.user_id
    {% endif %}
       else 'f' || events.domain_userid
  end as customer_id,
  derived_tstamp as conversion_tstamp,
  {{ conversion_value() }} as revenue

from {{ var('snowplow__conversions_source' )}} as events

{% if var('snowplow__use_snowplow_web_user_mapping_table') %}
  left join {{ var('snowplow__web_user_mapping_table') }} as user_mapping
    on events.domain_userid = user_mapping.domain_userid
{% endif %}

where {{ conversion_clause() }}
  and date(derived_tstamp) >= '{{ get_lookback_date_limits("min") }}'
  and date(derived_tstamp) <= '{{ get_lookback_date_limits("max") }}'
  and date({{ var('snowplow__conversions_source_filter') }}) >= {{ dateadd('day',-var('snowplow__conversions_source_filter_buffer_days'), "'"~get_lookback_date_limits('min')~"'") }}
  and date({{ var('snowplow__conversions_source_filter') }}) <= {{ dateadd('day', var('snowplow__conversions_source_filter_buffer_days'),"'"~get_lookback_date_limits('max')~"'") }}
