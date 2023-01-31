{% macro path_transformation(transformation_type, transform_param) %}
  {{ return(adapter.dispatch('path_transformation', 'snowplow_fractribution')(transformation_type, transform_param)) }}
{% endmacro %}

-- only used for integration tests
{% macro default__path_transformation(transformation_type, transform_param) %}

    {{schema}}.{{transformation_type}}(

      transformed_path

    {% if transform_param %}, '{{transform_param}}' {% endif %}
    )

{% endmacro %}

{% macro databricks__path_transformation(transformation_type, transform_param) %}

  {% if transformation_type == 'unique_path' %}
    transformed_path

  {% elif transformation_type == 'frequency_path' %}
    array_distinct(transform(transformed_path, element -> concat(element, "(", array_size(transformed_path)-array_size(array_remove(transformed_path, element )), ")" )))

  {% elif transformation_type == 'first_path' %}
    array_distinct(transformed_path)

  {% elif transformation_type == 'exposure_path' %}
    filter(transformed_path, (x, i) -> x != transformed_path[i-1] or i == 0)

  {% elif transformation_type == 'remove_if_not_all' %}
    case when array_distinct(transformed_path) != array('{{ transform_param }}')
    then array_remove(transformed_path, '{{ transform_param }}')
    else transformed_path end

  {% elif transformation_type == 'remove_if_last_and_not_all' %}
    case when array_distinct(transformed_path) != array('{{ transform_param }}')
    then slice(transformed_path, 1, array_size(transformed_path) - array_size(
    filter(transformed_path, (x, i) -> array_except(slice(reverse(transformed_path), 1, i), array('{{ transform_param }}'))==array()) ) + 1)
    else transformed_path end

  {% else %}
    {%- do exceptions.raise_compiler_error("Snowplow Warning: the path transform - '"+transformation_type+"' - is not yet supported for Databricks. Please choose from the following: exposure_path, first_path, remove_if_not_all, unique_path") %}

  {% endif %}

{% endmacro %}
