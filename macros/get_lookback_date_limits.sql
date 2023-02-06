{% macro get_lookback_date_limits(limit_type) %}
  {{ return(adapter.dispatch('get_lookback_date_limits', 'snowplow_fractribution')(limit_type)) }}
{% endmacro %}

{% macro default__get_lookback_date_limits(limit_type) %}

  -- check if web data is up-to-date

  {% set query %}
    select max(start_tstamp) < '{{ var('conversion_window_end_date') }}' as is_over_limit
    from {{ var('page_views_source') }}
  {% endset %}

  {% set result = run_query(query) %}

  {% if execute %}
    {% set page_view_max = result[0][0] %}
    {% if page_view_max == True %}
      {%- do exceptions.raise_compiler_error("Snowplow Warning: the derived.page_view source does not cover the full fractribution analysis period. Please process your web model first before proceeding with this package.") %}
    {% endif %}
  {% endif %}



  {% set query %}
    {% if limit_type == 'min' %}
      with base as (select case when '{{ var("conversion_window_start_date") }}' = ''
                  then {{ dbt.dateadd('day', -31, dbt.current_timestamp()) }}
                  else '{{ var("conversion_window_start_date") }}'
                  end as min_date_time)
      select cast({{ dbt.dateadd('day', (- var('path_lookback_days') + 1), 'min_date_time') }} as date) from base


    {% elif limit_type == 'max' %}
      with base as (select case when '{{ var("conversion_window_start_date") }}' = ''
                  then {{ dbt.dateadd('day', -1, dbt.current_timestamp()) }}
                  else '{{ var('conversion_window_end_date') }}'
                  end as max_date_time)
      select cast(max_date_time as date) from base
    {% else %}
    {% endif %}
  {% endset %}

  {% set query_result = run_query(query) %}

  {% if execute %}
    {% set result = query_result[0][0] %}
    {{ return(result) }}
  {% endif %}

{% endmacro %}
