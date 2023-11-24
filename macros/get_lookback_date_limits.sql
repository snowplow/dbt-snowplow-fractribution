{% macro get_lookback_date_limits(limit_type, model) %}
  {{ return(adapter.dispatch('get_lookback_date_limits', 'snowplow_fractribution')(limit_type, model)) }}
{% endmacro %}

{% macro default__get_lookback_date_limits(limit_type, model) %}

  -- check if web data is up-to-date in page_views_source (should cover conversion source check in case the web model is used to model conversions)
  {% set combined_time = var("snowplow__conversion_window_days") + var('snowplow__path_lookback_days') %}
  {% set query %}

      -- when the user opts for the auto-populated conversion window
      {% if var("snowplow__conversion_window_start_date") == '' and var("snowplow__conversion_window_end_date") == '' %}
           with base as (
              select max(start_tstamp) as last_pageview,
                    min(start_tstamp) as first_pageview
              from {{ var('snowplow__page_views_source') }}
           )
           select
             false as is_over_limit, -- the last pageview will be taken from the page_views_source
             cast(first_pageview as date) > cast({{ dbt.dateadd('day', -combined_time, 'last_pageview') }} as date) as is_below_limit,
             cast(last_pageview as {{ type_string() }}) as last_processed_page_view,
             cast(first_pageview as {{ type_string() }}) as first_processed_page_view
          from base
      -- when the user opts for manually defined conversion window
      {% elif var("snowplow__conversion_window_start_date")|length and var("snowplow__conversion_window_end_date")|length %}
           select
             max(start_tstamp) < '{{ var('snowplow__conversion_window_end_date') }}' as is_over_limit,
             cast(min(start_tstamp) as date) > '{{ var("snowplow__conversion_window_start_date") }}' as is_below_limit,
             cast(max(start_tstamp) as {{ type_string() }}) as last_processed_page_view,
             cast(min(start_tstamp) as {{ type_string() }}) as first_processed_page_view
          from {{ var('snowplow__page_views_source') }}

      {% else %}
        {%- do exceptions.raise_compiler_error("Snowplow Error: please either give both of the following variables a value or set both as empty strings: snowplow__conversion_window_start_date & snowplow__conversion_window_end_date ") %}


      {% endif %}

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

  -- setting and executing the limit query depending on input

  {% set query %}
    {% if limit_type == 'min' and model == 'sessions' %}
      {% if var("snowplow__conversion_window_start_date") == '' and var("snowplow__conversion_window_end_date") == '' %}
          with base as (
            select max(start_tstamp) as last_pageview
            from {{ var('snowplow__page_views_source') }}
          )
          select cast({{ dbt.dateadd('day', -combined_time, 'last_pageview') }} as date) as lower_limit
          from base

      {% else %}
         with base as (
          select cast('{{ var('snowplow__conversion_window_start_date')}}' as timestamp) as cw_tstamp
         )
        select cast({{ dbt.dateadd('day', -var('snowplow__path_lookback_days'), 'cw_tstamp' ) }} as date) as lower_limit
        from base
      {% endif %}

    {% elif limit_type == 'max' and model == 'sessions' %}
      {% if var("snowplow__conversion_window_start_date") == '' and var("snowplow__conversion_window_end_date") == '' %}
          with base as (
                select max(start_tstamp) as last_pageview
                from {{ var('snowplow__page_views_source') }}
            )
        select cast({{ dbt.dateadd('day', -1, 'last_pageview') }} as date) as upper_limit
        from base

      {% else %}
        select cast('{{ var("snowplow__conversion_window_end_date") }}' as date) as upper_limit
      {% endif %}

    {% elif limit_type == 'min' and model == 'conversions' %}
      {% if var("snowplow__conversion_window_start_date") == '' and var("snowplow__conversion_window_end_date") == '' %}
         with base as (
            select max(start_tstamp) as last_pageview
            from {{ var('snowplow__page_views_source') }}
         )
        select cast( {{ dbt.dateadd('day', -var("snowplow__conversion_window_days"), 'last_pageview') }} as date) as lower_limit
          from base

      {% else %}
        select cast('{{ var("snowplow__conversion_window_start_date") }}' as date) as lower_limit
      {% endif %}

    {% elif limit_type == 'max' and model == 'conversions' %}
      {% if var("snowplow__conversion_window_start_date") == '' and var("snowplow__conversion_window_end_date") == '' %}
        with base as (
                select max(start_tstamp) as last_pageview
                from {{ var('snowplow__page_views_source') }}
            )
        select cast({{ dbt.dateadd('day', -1, 'last_pageview') }} as date) as upper_limit
        from base

      {% else %}
        select cast('{{ var("snowplow__conversion_window_end_date") }}' as date) as upper_limit
      {% endif %}

    {% else %}
    {% endif %}
  {% endset %}

  {% set query_result = run_query(query) %}

  {% if execute %}
    {% set result = query_result[0][0] %}
    {{ return(result) }}
  {% endif %}

{% endmacro %}
