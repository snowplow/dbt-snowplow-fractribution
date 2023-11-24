{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

--This model is only used when the attribution package is run on Snowflake and the Python script will be run using Snowpark, rather than manually.
{{ config(
    enabled = target.type == "snowflake" and var('snowplow__run_python_script_in_snowpark', false),
    materialized = 'table',
)}}

-- depends_on: {{ref('snowplow_fractribution_path_summary')}}
-- depends_on: {{ref('snowplow_fractribution_channel_spend')}}

{{create_report_table_proc()}}
{{run_stored_procedure(var('snowplow__attribution_model_for_snowpark'), var('snowplow__conversion_window_start_date') , var('snowplow__conversion_window_end_date') )}}
With table_1 as (
    SELECT
        '{{schema}}' as schema_name,
         'snowplow_fractribution_path_summary_with_channels' as table_name,
        '{{dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S")}}' as last_run_time
),
table_2 as (
    SELECT
        '{{schema}}' as schema_name,
         'snowplow_fractribution_channel_attribution' as table_name,
        '{{dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S")}}' as last_run_time
),
table_3 as (
    SELECT
        '{{schema}}' as schema_name,
         'snowplow_fractribution_report_table' as table_name,
        '{{dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S")}}' as last_run_time
)
SELECT * FROM table_1
UNION ALL
SELECT * FROM table_2
UNION ALL
SELECT * FROM table_3


{%- if execute %}
{{ log('Finished running stored procedure. Created tables:
snowplow_fractribution_path_summary_with_channels
snowplow_fractribution_channel_attribution
snowplow_fractribution_report_table', info=True)}}
{% endif %}
