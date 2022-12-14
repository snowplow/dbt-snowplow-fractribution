{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

SELECT
    CASE
        WHEN page_views.user_id IS NOT NULL AND page_views.user_id != '' THEN 'u' || page_views.user_id -- use event user_id
        {% if var('use_snowplow_web_user_mapping_table') %}
            WHEN user_mapping.domain_userid IS NOT NULL THEN 'u' || user_mapping.user_id
        {% endif %}
        ELSE 'f' || page_views.domain_userid
    END AS customerId, -- f (anonymous) or u (identifier) prefixed user identifier
    derived_tstamp AS visitStartTimestamp, -- we consider the event timestamp to be the session start, rather than the session start timestamp
    {{ channel_classification() }} AS channel,
    refr_urlpath AS referralPath,
    mkt_campaign AS campaign,
    mkt_source AS source,
    mkt_medium AS medium
FROM
    {{ var('page_views_source') }}  page_views
    {% if var('use_snowplow_web_user_mapping_table') %}
        LEFT JOIN
        {{ var('snowplow_web_user_mapping_table') }} AS user_mapping
        ON
        page_views.domain_userid = user_mapping.domain_userid
    {% endif %}
WHERE
    DATE(derived_tstamp) >= DATEADD(d, -{{ var('path_lookback_days') + 1 }},
                                CASE WHEN '{{ var('conversion_window_start_date') }}' = ''
                                THEN current_date()-31
                                ELSE '{{ var('conversion_window_start_date') }}'
                                END)
    AND
    DATE(derived_tstamp) <= CASE WHEN '{{ var('conversion_window_end_date') }}' = ''
                                THEN current_date()-1
                                ELSE '{{ var('conversion_window_end_date') }}'
                                END
    AND
    -- restrict to certain hostnames
    {% if var('conversion_hosts') in ('', [], '[]') or var('conversion_hosts') == None %}
      {{ exceptions.raise_compiler_error("Error: var('conversion_host') needs to be set!") }}

    {% endif %}
    page_urlhost IN (
        {%- for urlhost in var('conversion_hosts') %}
            '{{ urlhost }}'
            {%- if not loop.last %},{% endif %}
        {%- endfor %}
    )

    {% if var('consider_intrasession_channels') %}
        -- yields one row per channel change
        AND mkt_medium IS NOT NULL AND mkt_medium != ''
    {% else %}
        -- yields one row per session (last touch)
        AND page_view_in_session_index = 1 -- takes the first page view in the session
    {% endif %}

