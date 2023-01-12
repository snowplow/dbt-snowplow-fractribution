/* Macro to perform channel classifications
   each channel should return a name that will also be a valid column name by convention use underscores, avoid spaces,
   leading numbers) */

{% macro channel_classification() %}
    {{ return(adapter.dispatch('channel_classification', 'snowplow_fractribution')()) }}
{% endmacro %}

{% macro default__channel_classification() %}

    case when lower(mkt_medium) in ('cpc', 'ppc') and regexp_count(lower(mkt_campaign), 'brand') > 0 then 'Paid_Search_Brand'
         when lower(mkt_medium) in ('cpc', 'ppc') and regexp_count(lower(mkt_campaign), 'generic') > 0 then 'Paid_Search_Generic'
         when lower(mkt_medium) in ('cpc', 'ppc') and not regexp_count(lower(mkt_campaign), 'brand|generic') > 0 then 'Paid_Search_Other'
         when lower(mkt_medium) = 'organic' then 'Organic_Search'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and regexp_count(lower(mkt_campaign), 'prospect') > 0 then 'Display_Prospecting'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and regexp_count(lower(mkt_campaign), 'retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Display_Retargeting'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and not regexp_count(lower(mkt_campaign), 'prospect|retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Display_Other'
         when regexp_count(lower(mkt_campaign), 'video|youtube') > 0 or regexp_count(lower(mkt_source), 'video|youtube') > 0 then 'Video'
         when lower(mkt_medium) = 'social' and regexp_count(lower(mkt_campaign), 'prospect') > 0 then 'Paid_Social_Prospecting'
         when lower(mkt_medium) = 'social' and regexp_count(lower(mkt_campaign), 'retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Paid_Social_Retargeting'
         when lower(mkt_medium) = 'social' and not regexp_count(lower(mkt_campaign), 'prospect|retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Paid_Social_Other'
         when mkt_source = '(direct)' then 'Direct'
         when lower(mkt_medium) = 'referral' then 'Referral'
         when lower(mkt_medium) = 'email' then 'Email'
         when lower(mkt_medium) in ('cpc', 'ppc', 'cpv', 'cpa', 'affiliates') then 'Other_Advertising'
         else 'Unmatched_Channel'
    end

{% endmacro %}


{% macro bigquery__channel_classification() %}

    case when lower(mkt_medium) in ('cpc', 'ppc') and array_length(regexp_extract_all(lower(mkt_campaign), 'brand')) > 0 then 'Paid_Search_Brand'
         when lower(mkt_medium) in ('cpc', 'ppc') and array_length(regexp_extract_all(lower(mkt_campaign), 'generic')) > 0 then 'Paid_Search_Generic'
         when lower(mkt_medium) in ('cpc', 'ppc') and not array_length(regexp_extract_all(lower(mkt_campaign), 'brand|generic')) > 0 then 'Paid_Search_Other'
         when lower(mkt_medium) = 'organic' then 'Organic_Search'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and array_length(regexp_extract_all(lower(mkt_campaign), 'prospect')) > 0 then 'Display_Prospecting'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and array_length(regexp_extract_all(lower(mkt_campaign), 'retargeting|re-targeting|remarketing|re-marketing')) > 0 then 'Display_Retargeting'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and not array_length(regexp_extract_all(lower(mkt_campaign), 'prospect|retargeting|re-targeting|remarketing|re-marketing')) > 0 then 'Display_Other'
         when array_length(regexp_extract_all(lower(mkt_campaign), 'video|youtube')) > 0 or array_length(regexp_extract_all(lower(mkt_source), 'video|youtube')) > 0 then 'Video'
         when lower(mkt_medium) = 'social' and array_length(regexp_extract_all(lower(mkt_campaign), 'prospect')) > 0 then 'Paid_Social_Prospecting'
         when lower(mkt_medium) = 'social' and array_length(regexp_extract_all(lower(mkt_campaign), 'retargeting|re-targeting|remarketing|re-marketing')) > 0 then 'Paid_Social_Retargeting'
         when lower(mkt_medium) = 'social' and not array_length(regexp_extract_all(lower(mkt_campaign), 'prospect|retargeting|re-targeting|remarketing|re-marketing')) > 0 then 'Paid_Social_Other'
         when mkt_source = '(direct)' then 'Direct'
         when lower(mkt_medium) = 'referral' then 'Referral'
         when lower(mkt_medium) = 'email' then 'Email'
         when lower(mkt_medium) in ('cpc', 'ppc', 'cpv', 'cpa', 'affiliates') then 'Other_Advertising'
         else 'Unmatched_Channel'
    end

{% endmacro %}
