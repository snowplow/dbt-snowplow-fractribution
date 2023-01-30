{% macro get_lookback_date_limits(limit_type) %}

  {% if limit_type == 'min' %}

    {% set query %}
    with base as (select case when '{{ var('conversion_window_start_date') }}' = ''
                then {{ dbt.dateadd('day', -31, dbt.current_timestamp()) }}
                else '{{ var('conversion_window_start_date') }}'
                end as min_date_time)
    select cast({{ dbt.dateadd('day', (- var('path_lookback_days') + 1), 'min_date_time') }} as date) from base

    {% endset %}

    {% set query_result = run_query(query) %}

    {% if execute %}
      {% set result = query_result[0][0] %}
      {{ return(result) }}
    {% endif %}

  {% elif limit_type == 'max' %}

   {% set query %}
    with base as (select case when '{{ var('conversion_window_start_date') }}' = ''
                then {{ dbt.dateadd('day', -2, dbt.current_timestamp()) }}
                else '{{ var('conversion_window_end_date') }}'
                end as max_date_time)
    select cast(max_date_time as date) from base
    {% endset %}

    {% set query_result = run_query(query) %}

    {% if execute %}
      {% set result = query_result[0][0] %}
      {{ return(result) }}
    {% endif %}

  {% else %}
  {% endif %}

{% endmacro %}
