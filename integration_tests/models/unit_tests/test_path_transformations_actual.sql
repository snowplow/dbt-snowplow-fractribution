{% if target.type == 'bigquery' -%}

  with raw_data as (

      select * from {{ ref('test_path_transformations_data') }}

    )

    , arrays as (
      select
        transformed_path as raw_array,
        {{ snowplow_fractribution.trim_long_path('transformed_path', 1) }} as trim_long_path,
        {{ snowplow_fractribution.path_transformation('unique_path') }} as unique_path,
        {{ snowplow_fractribution.path_transformation('frequency_path', '') }} as frequency_path,
        {{ snowplow_fractribution.path_transformation('exposure_path', '') }} as exposure_path,
        {{ snowplow_fractribution.path_transformation('first_path') }} as first_path,
        {{ snowplow_fractribution.path_transformation('remove_if_not_all', 'Direct') }} as remove_if_not_all,
        {{ snowplow_fractribution.path_transformation('remove_if_last_and_not_all', 'Direct') }} as remove_if_last_and_not_all

    from raw_data d
    )

    select
      to_json_string(raw_array) as raw_array,
      to_json_string(trim_long_path) as trim_long_path,
      to_json_string(unique_path) as unique_path,
      to_json_string(frequency_path) as frequency_path,
      to_json_string(exposure_path) as exposure_path,
      to_json_string(first_path) as first_path,
      to_json_string(remove_if_not_all) as remove_if_not_all,
      to_json_string(remove_if_last_and_not_all) as remove_if_last_and_not_all

    from arrays

{% else %}

  with raw_data as (

      select * from {{ ref('test_path_transformations_data') }}

    )

    select
      transformed_path as raw_array,
      {{ snowplow_fractribution.trim_long_path('transformed_path', 1) }} as trim_long_path,
      {{ snowplow_fractribution.path_transformation('unique_path') }} as unique_path,
      {{ snowplow_fractribution.path_transformation('frequency_path', '') }} as frequency_path,
      {{ snowplow_fractribution.path_transformation('exposure_path', '') }} as exposure_path,
      {{ snowplow_fractribution.path_transformation('first_path') }} as first_path,
      {{ snowplow_fractribution.path_transformation('remove_if_not_all', 'Direct') }} as remove_if_not_all,
      {{ snowplow_fractribution.path_transformation('remove_if_last_and_not_all', 'Direct') }} as remove_if_last_and_not_all

    from raw_data d

{% endif %}
