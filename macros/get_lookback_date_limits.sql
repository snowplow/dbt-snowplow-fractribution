{% macro get_lookback_date_limits(limit_type) %}
  {{ return(adapter.dispatch('get_lookback_date_limits', 'snowplow_fractribution')(limit_type)) }}
{% endmacro %}

{% macro default__get_lookback_date_limits(limit_type) %}

  -- check if web data is up-to-date

  {% set query %}
    select max(start_tstamp) < '{{ var('snowplow__conversion_window_end_date') }}' as is_over_limit,
           cast(min(start_tstamp) as date) > '{{ var("snowplow__conversion_window_start_date") }}' as is_below_limit,
           cast(max(start_tstamp) as {{ type_string() }}) as last_processed_page_view,
           cast(min(start_tstamp) as {{ type_string() }}) as first_processed_page_view
    from {{ var('snowplow__page_views_source') }}
  {% endset %}

  {% set result = run_query(query) %}

  {% if execute %}
    {% set page_view_max = result[0][0] %}
    {% set last_processed_page_view = result[0][2] %}
    {% if page_view_max == True %}
      {%- do exceptions.raise_compiler_error("Snowplow Error: the derived.page_view source does not cover the full fractribution analysis period.
                                              Please process your web model first before proceeding with this package. Details: snowplow__conversion_window_start_date "
                                              + var('snowplow__conversion_window_end_date') + " is later than last processed pageview " + last_processed_page_view) %}
    {% endif %}
    {% set page_view_min = result[0][1] %}
    {% set first_processed_page_view = result[0][3] %}
    {% if page_view_min == True %}
      {%- do exceptions.raise_compiler_error("Snowplow Error: the derived.page_view source does not cover the full fractribution analysis period.
                                              Please backfill / reprocess your web model first before proceeding with this package. Details: snowplow__conversion_window_start_date "
                                              + var('snowplow__conversion_window_start_date') + " is earlier than first processed pageview " + first_processed_page_view) %}
    {% endif %}
  {% endif %}


  {% set query %}
    {% if limit_type == 'min' %}
      with base as (select case when '{{ var("snowplow__conversion_window_start_date") }}' = ''
                  then {{ dbt.dateadd('day', -31, dbt.current_timestamp()) }}
                  else '{{ var("snowplow__conversion_window_start_date") }}'
                  end as min_date_time)
      select cast({{ dbt.dateadd('day', (- var('snowplow__path_lookback_days') + 1), 'min_date_time') }} as date) from base


    {% elif limit_type == 'max' %}
      with base as (select case when '{{ var("snowplow__conversion_window_start_date") }}' = ''
                  then {{ dbt.dateadd('day', -1, dbt.current_timestamp()) }}
                  else '{{ var("snowplow__conversion_window_end_date") }}'
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
