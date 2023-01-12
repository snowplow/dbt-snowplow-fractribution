-- page view context is given as json string in csv. Parse json
with prep as (
select
  *
   except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0),
   JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) AS contexts_com_snowplowanalytics_snowplow_web_page_1_0_0

from {{ ref('snowplow_fractribution_events') }}
)

select
  *
  except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0),
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.id') as id
    from unnest(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_snowplow_web_page_1_0_0

from prep
