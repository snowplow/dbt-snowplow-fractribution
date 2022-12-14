{{ 
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  ) 
}}

SELECT
    CASE
        WHEN events.user_id IS NOT NULL AND events.user_id != '' THEN 'u' || events.user_id -- use event user_id
        {% if var('use_snowplow_web_user_mapping_table') %}
            WHEN user_mapping.domain_userid IS NOT NULL THEN 'u' || user_mapping.user_id
        {% endif %}
        ELSE 'f' || events.domain_userid
    END AS customerId,
    derived_tstamp AS conversionTimestamp,
    {{ conversion_value() }} AS revenue
FROM
    {{ var('conversions_source' )}} AS events
    {% if var('use_snowplow_web_user_mapping_table') %}
        LEFT JOIN
        {{ var('snowplow_web_user_mapping_table') }} AS user_mapping
        ON
        events.domain_userid = user_mapping.domain_userid
    {% endif %}
WHERE
    {{ conversion_clause() }}
    AND
    DATE(derived_tstamp) >= CASE WHEN '{{ var('conversion_window_start_date') }}' = '' 
                                THEN current_date()-31
                                ELSE '{{ var('conversion_window_start_date') }}'
                                END
    AND
    DATE(derived_tstamp) <= CASE WHEN '{{ var('conversion_window_end_date') }}' = '' 
                                THEN current_date()-1
                                ELSE '{{ var('conversion_window_end_date') }}'
                                END
