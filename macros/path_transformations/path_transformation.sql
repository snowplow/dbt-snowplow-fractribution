/* Macro to remove complexity from models paths_to_conversion / paths_to_non_conversion. */

{% macro path_transformation(transformation_type, transform_param, source_table) %}
  {{ return(adapter.dispatch('path_transformation', 'snowplow_fractribution')(transformation_type, transform_param, source_table)) }}
{% endmacro %}


{% macro default__path_transformation(transformation_type, transform_param, source_table) %}

  {% if transformation_type == 'unique_path' %}
    transformed_path

  {% elif transformation_type == 'frequency_path' %}
    array_distinct(transform(transformed_path, element -> concat(element, "(", array_size(transformed_path)-array_size(array_remove(transformed_path, element )), ")" )))

  {% elif transformation_type == 'first_path' %}
    array_distinct(transformed_path)

  {% elif transformation_type == 'exposure_path' %}
    filter(transformed_path, ({{ source_table }}, i) -> {{ source_table }} != transformed_path[i-1] or i == 0)

  {% elif transformation_type == 'remove_if_not_all' %}
    case when array_distinct(transformed_path) != array('{{ transform_param }}')
    then array_remove(transformed_path, '{{ transform_param }}')
    else transformed_path end

  {% elif transformation_type == 'remove_if_last_and_not_all' %}
    case when array_distinct(transformed_path) != array('{{ transform_param }}')
    then slice(transformed_path, 1, array_size(transformed_path) - array_size(
    filter(transformed_path, ({{ source_table }}, i) -> array_except(slice(reverse(transformed_path), 1, i), array('{{ transform_param }}'))==array()) ) + 1)
    else transformed_path end

  {% else %}
    {%- do exceptions.raise_compiler_error("Snowplow Warning: the path transform - '"+transformation_type+"' - is not yet supported for Databricks. Please choose from the following: exposure_path, first_path, remove_if_not_all, unique_path") %}

  {% endif %}

{% endmacro %}


-- only used for integration tests
{% macro snowflake__path_transformation(transformation_type, transform_param, source_table) %}

    {{schema}}.{{transformation_type}}(

      transformed_path

    {% if transform_param %}, '{{transform_param}}' {% endif %}
    )

{% endmacro %}
