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
