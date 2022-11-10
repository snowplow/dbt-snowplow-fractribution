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
    {{ source('atomic', 'events') }} AS events
    {% if var('use_snowplow_web_user_mapping_table') %}
        LEFT JOIN
        {{ var('snowplow_web_user_mapping_table') }} AS user_mapping
        ON
        events.domain_userid = user_mapping.domain_userid
    {% endif %}
WHERE
    {{ conversion_clause() }}
    AND
    DATE(derived_tstamp) >= '{{ var('conversion_window_start_date') }}'
    AND
    DATE(derived_tstamp) <= '{{ var('conversion_window_end_date') }}'
