{% macro path_transformation(transformation_type, transform_param) %}
  {{ return(adapter.dispatch('path_transformation', 'snowplow_fractribution')(transformation_type, transform_param)) }}
{% endmacro %}

-- only used for integration tests
{% macro default__path_transformation(transformation_type, transform_param) %}

    {{target.schema}}.{{transformation_type}}(

      transformed_path

    {% if transform_param %}, '{{transform_param}}' {% endif %}
    )

{% endmacro %}

{% macro spark__path_transformation(transformation_type, transform_param) %}

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
    /* remove the matching path(s) from the tail unless it removes everything (obtaining the upper boundary of the
    slicing to do this is done by slicing the array and determining if it only contains the desired references which
    it then returns an element for only if they are equivalent.) 
    Example:
        ["Example", "Another", "Direct", "Direct"]
        filter(y, (x, i) -> array_except(slice(reverse(y), 1, i), array('Direct'))==array())
        
        Slice 1 (i=1): Direct.
        array_except yields [] as our array only contains 'Direct' references, comparison yields True
        Slice 2 (i=2): Direct, Direct
        array_except yields [], comparison yields True
        Slice 3 (i=3): Direct, Direct, Another
        array_except yields [Another], comparison yields False (element does not become part of the array)
        Slice 4 (i=4): Direct, Direct, Another, Example
        array_except yields [Another, Example], comparison yields False (element does not become part of the array)
        
        At this point we can now count the size of this array - which gives us an index (from the back of the array) as to how many elements we can chop off - so to convert this to a an actual slice (as negative slicing sort of works in DB) we do:
        array_size(original) - array_size(direct_size) + 1
        4 - 2 + 1 = 3   
    */
    case when array_distinct(transformed_path) != array('{{ transform_param }}')
    then slice(transformed_path, 1, array_size(transformed_path) - array_size(
    filter(transformed_path, (x, i) -> array_except(slice(reverse(transformed_path), 1, i), array('{{ transform_param }}'))==array()) ) + 1)
    else transformed_path end

  {% else %}
    {%- do exceptions.raise_compiler_error("Snowplow Error: the path transform - '"+transformation_type+"' - is not yet supported for Databricks. Please choose from the following: exposure_path, first_path, frequency_path, remove_if_last_and_not_all, remove_if_not_all, unique_path") %}

  {% endif %}

{% endmacro %}
