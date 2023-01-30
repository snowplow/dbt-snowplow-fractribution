/* Macro to remove complexity from models paths_to_conversion / paths_to_non_conversion. */

{% macro transform_paths(model_type) %}
  {{ return(adapter.dispatch('transform_paths', 'snowplow_fractribution')(model_type)) }}
{% endmacro %}

{% macro default__transform_paths(model_type) %}

  {% set allowed_path_transforms = ['exposure_path', 'first_path', 'frequency_path', 'remove_if_last_and_not_all', 'remove_if_not_all', 'unique_path'] %}

  , path_transforms as (

     select
        customer_id,
        {% if model_type == 'conversions' %}
        conversion_tstamp,
        revenue,
        {% endif %}
        {{ trim_long_path('path') }} as path,

    {% if var('path_transforms').items()|length > 0 %}

      {% for path_transform_name, _ in var('path_transforms').items()|reverse %}
        {% if path_transform_name not in allowed_path_transforms %}
          {%- do exceptions.raise_compiler_error("Snowplow Warning: the path transform - '"+path_transform_name+"' - is not supported. Please refer to the Snowplow docs on tagging. Please use one of the following: exposure_path, first_path, frequency_path, remove_if_last_and_not_all, remove_if_not_all, unique_path") %}
        {% endif %}
        {{schema}}.{{path_transform_name}}(
      {% endfor %}

      transformed_path

      {% for _, transform_param in var('path_transforms').items()|reverse %}
        {% if transform_param %}, '{{transform_param}}' {% endif %}
        )
      {% endfor %}

      as transformed_path

    {% else %}
     transformed_path
    {% endif %}

  from arrays

  )

{% endmacro %}


{% macro databricks__transform_paths(model_type) %}

  {% set total_transformations = var('path_transforms').items()|length %}
  {% set loop_count = namespace(value=1) %}

  {% for path_transform_name, transform_param in var('path_transforms').items() %}

    {%- if loop_count.value == 1 %}
      {% set previous_cte = 'arrays' %}
    {% else %}
      {% set previous_cte = loop_count.value-1 %}
    {% endif %}

    , transformation_{{ loop_count.value|string }} as (

      select
        customer_id,
        {% if model_type == 'conversions' %}
        conversion_tstamp,
        revenue,
        {% endif %}
        path,
        {% if path_transform_name == 'unique_path' %}
          {{ path_transformation('unique_path') }} as transformed_path

        {% elif path_transform_name == 'frequency_path' %}
          {{ path_transformation('frequency_path', '', 'transformation_' + previous_cte.value|string) }} as transformed_path

        {% elif path_transform_name == 'first_path' %}
          {{ path_transformation('first_path') }} as transformed_path

        {% elif path_transform_name == 'exposure_path' %}
          {{ path_transformation('exposure_path', '', 'transformation_' + previous_cte.value|string) }} as transformed_path

        {% elif path_transform_name == 'remove_if_not_all' %}
          {{ path_transformation('remove_if_not_all', transform_param) }} as transformed_path

        {% elif path_transform_name == 'remove_if_last_and_not_all' %}
          {{ path_transformation('remove_if_last_and_not_all', transform_param, 'transformation_' + previous_cte.value|string) }} as transformed_path

        {% else %}
          {%- do exceptions.raise_compiler_error("Snowplow Warning: the path transform - '"+path_transform_name+"' - is not supported. Please refer to the Snowplow docs on tagging. Please use one of the following: exposure_path, first_path, frequency_path, remove_if_last_and_not_all, remove_if_not_all, unique_path") %}
        {% endif %}

        {%- if loop_count.value == 1 %}
         from arrays
         )
        {% else %}
         from transformation_{{ previous_cte|string }}
         )
        {% endif %}
        {% set loop_count.value = loop_count.value + 1 %}
        {% set previous_cte = loop_count.value-1 %}

  {% endfor %}

  , path_transforms as (

    select
      customer_id,
      {% if model_type == 'conversions' %}
      conversion_tstamp,
      revenue,
      {% endif %}
      {{ trim_long_path('path') }} as path,
      transformed_path

  {% if total_transformations > 0 %}
    from transformation_{{ total_transformations }}

  {% else %}
    from arrays
  {% endif %}
  )

{% endmacro %}
