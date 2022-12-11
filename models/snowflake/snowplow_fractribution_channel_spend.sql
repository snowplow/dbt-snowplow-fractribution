{{ 
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  ) 
}}

-- By default, the model assigns an example 10k spend to each channel found in channel_counts
-- TODO: put in your own spend calculations per channel in the channel_spend macro in your own dbt project


{{ channel_spend() }}