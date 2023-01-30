/* Returns the last 'path_lookback_steps' number of channels in the path if path_lookback_steps > 0,
   or the full path otherwise. */

{% macro trim_long_path(array_column, lookback_steps=var('path_lookback_steps')) %}
  {{ return(adapter.dispatch('trim_long_path', 'snowplow_fractribution')(array_column,lookback_steps)) }}
{% endmacro %}


{% macro default__trim_long_path(array_column, lookback_steps=var('path_lookback_steps')) %}

  {% if lookback_steps > 0 %}

    slice(reverse({{ array_column }}), 1, (greatest(1, (cast({{ lookback_steps }} as int)))))

  {% else %}

    {{ array_column }}

  {% endif %}

{% endmacro %}


{% macro snowflake__trim_long_path(array_column, lookback_steps=var('path_lookback_steps')) %}

  {{ schema }}.trim_long_path({{array_column}}, {{ lookback_steps }})

{% endmacro %}

