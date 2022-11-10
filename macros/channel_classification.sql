{% macro channel_classification() %}
    {{ return(adapter.dispatch('channel_classification', 'fractribution')()) }}
{% endmacro %}

{% macro snowflake__channel_classification() %}
    -- macro to perform channel classifications
    -- each channel should return a name that will also be a valid Snowflake column name
    -- by convention use underscores to separate
    -- (<251 characters, avoid spaces, leading numbers)

    CASE
        WHEN
            LOWER(mkt_medium) IN ('cpc', 'ppc')
            AND REGEXP_COUNT(LOWER(mkt_campaign), 'brand') > 0
            THEN 'Paid_Search_Brand'
        WHEN
            LOWER(mkt_medium) IN ('cpc', 'ppc')
            AND REGEXP_COUNT(LOWER(mkt_campaign), 'generic') > 0
            THEN 'Paid_Search_Generic'
        WHEN
            LOWER(mkt_medium) IN ('cpc', 'ppc')
            AND NOT REGEXP_COUNT(LOWER(mkt_campaign), 'brand|generic') > 0
            THEN 'Paid_Search_Other'
        WHEN LOWER(mkt_medium) = 'organic' THEN 'Organic_Search'
        WHEN
            LOWER(mkt_medium) IN ('display', 'cpm', 'banner')
            AND REGEXP_COUNT(LOWER(mkt_campaign), 'prospect') > 0
            THEN 'Display_Prospecting'
        WHEN
            LOWER(mkt_medium) IN ('display', 'cpm', 'banner')
            AND REGEXP_COUNT(
                LOWER(mkt_campaign),
                'retargeting|re-targeting|remarketing|re-marketing') > 0
            THEN 'Display_Retargeting'
        WHEN
            LOWER(mkt_medium) IN ('display', 'cpm', 'banner')
            AND NOT REGEXP_COUNT(
                LOWER(mkt_campaign),
                'prospect|retargeting|re-targeting|remarketing|re-marketing') > 0
            THEN 'Display_Other'
        WHEN
            REGEXP_COUNT(LOWER(mkt_campaign), 'video|youtube') > 0
            OR REGEXP_COUNT(LOWER(mkt_source), 'video|youtube') > 0
            THEN 'Video'
        WHEN
            LOWER(mkt_medium) = 'social'
            AND REGEXP_COUNT(LOWER(mkt_campaign), 'prospect') > 0
            THEN 'Paid_Social_Prospecting'
        WHEN
            LOWER(mkt_medium) = 'social'
            AND REGEXP_COUNT(
                LOWER(mkt_campaign),
                'retargeting|re-targeting|remarketing|re-marketing') > 0
            THEN 'Paid_Social_Retargeting'
        WHEN
            LOWER(mkt_medium) = 'social'
            AND NOT REGEXP_COUNT(
                LOWER(mkt_campaign),
                'prospect|retargeting|re-targeting|remarketing|re-marketing') > 0
            THEN 'Paid_Social_Other'
        WHEN mkt_source = '(direct)' THEN 'Direct'
        WHEN LOWER(mkt_medium) = 'referral' THEN 'Referral'
        WHEN LOWER(mkt_medium) = 'email' THEN 'Email'
        WHEN
            LOWER(mkt_medium) IN ('cpc', 'ppc', 'cpv', 'cpa', 'affiliates')
            THEN 'Other_Advertising'
        ELSE 'Unmatched_Channel'
    END
{% endmacro %}
