{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

 -- restrict to certain hostnames
{% if var('conversion_hosts') in ('', [], '[]') or var('conversion_hosts') == None %}
    {{ exceptions.raise_compiler_error("Error: var('conversion_host') needs to be set!") }}
{% endif %}

select
  case when page_views.user_id is not null and page_views.user_id != '' then 'u' || page_views.user_id -- use event user_id
  {% if var('use_snowplow_web_user_mapping_table') %}
       when user_mapping.domain_userid is not null then 'u' || user_mapping.user_id
  {% endif %}
        else 'f' || page_views.domain_userid
  end as customer_id, -- f (anonymous) or u (identifier) prefixed user identifier
  derived_tstamp as visit_start_tstamp, -- we consider the event timestamp to be the session start, rather than the session start timestamp
  {{ channel_classification() }} as channel,
  refr_urlpath as referral_path,
  mkt_campaign as campaign,
  mkt_source as source,
  mkt_medium as medium

from {{ var('page_views_source') }}  page_views

{% if var('use_snowplow_web_user_mapping_table') %}
  left join {{ var('snowplow_web_user_mapping_table') }} as user_mapping
  on page_views.domain_userid = user_mapping.domain_userid
{% endif %}

where date(derived_tstamp) >= '{{ get_lookback_date_limits('min') }}'

    and date(derived_tstamp) <= '{{ get_lookback_date_limits('max') }}'

    and page_urlhost in (
        {%- for urlhost in var('conversion_hosts') %}
            '{{ urlhost }}'
            {%- if not loop.last %},{% endif %}
        {%- endfor %}
    )

{% if var('consider_intrasession_channels') %}
    -- yields one row per channel change
    and mkt_medium is not null and mkt_medium != ''

{% else %}
    -- yields one row per session (last touch)
    and page_view_in_session_index = 1 -- takes the first page view in the session

{% endif %}
