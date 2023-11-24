{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro run_stored_procedure(attribution_model, conversion_window_start_date, conversion_window_end_date) %}
  {{ return(adapter.dispatch('run_stored_procedure', 'snowplow_fractribution')(attribution_model, conversion_window_start_date, conversion_window_end_date)) }}
{% endmacro %}


{% macro default__run_stored_procedure(attribution_model, conversion_window_start_date, conversion_window_end_date) %}
{% endmacro %}


{% macro snowflake__run_stored_procedure(attribution_model, conversion_window_start_date, conversion_window_end_date) %}
{% if execute %}
{% set call_proc %}
CALL {{schema}}.create_report_table('{{attribution_model}}', '{{conversion_window_start_date}}', '{{conversion_window_end_date}}')
{% endset %}

{% do run_query(call_proc) %}
{% endif %}

  
{% endmacro %}
