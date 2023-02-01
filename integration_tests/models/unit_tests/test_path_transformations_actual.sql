with raw_data as (

    select * from {{ ref('test_path_transformations_data') }}

  )

  , arrays as (
    select
      transformed_path as raw_array,
      {{ snowplow_fractribution.trim_long_path('transformed_path', 1) }} as trim_long_path,
      {{ snowplow_fractribution.trim_long_path('transformed_path', 2) }} as trim_long_path2,
      {{ snowplow_fractribution.path_transformation('unique_path') }} as unique_path,
      {{ snowplow_fractribution.path_transformation('frequency_path', '') }} as frequency_path,
      {{ snowplow_fractribution.path_transformation('exposure_path', '') }} as exposure_path,
      {{ snowplow_fractribution.path_transformation('first_path') }} as first_path,
      {{ snowplow_fractribution.path_transformation('remove_if_not_all', 'Direct') }} as remove_if_not_all,
      {{ snowplow_fractribution.path_transformation('remove_if_last_and_not_all', 'Direct') }} as remove_if_last_and_not_all

  from raw_data d
  )

  select
    {{ snowplow_utils.get_array_to_string('raw_array', 'a', delimiter=', ') }} as raw_array,
    {{ snowplow_utils.get_array_to_string('trim_long_path', 'a', delimiter=', ') }} as trim_long_path,
    {{ snowplow_utils.get_array_to_string('trim_long_path2', 'a', delimiter=', ') }} as trim_long_path2,
    {{ snowplow_utils.get_array_to_string('unique_path', 'a', delimiter=', ') }} as unique_path,
    {{ snowplow_utils.get_array_to_string('frequency_path', 'a', delimiter=', ') }} as frequency_path,
    {{ snowplow_utils.get_array_to_string('exposure_path', 'a', delimiter=', ') }} as exposure_path,
    {{ snowplow_utils.get_array_to_string('first_path', 'a', delimiter=', ') }} as first_path,
    {{ snowplow_utils.get_array_to_string('remove_if_not_all', 'a', delimiter=', ') }} as remove_if_not_all,
    {{ snowplow_utils.get_array_to_string('remove_if_last_and_not_all', 'a', delimiter=',') }} as remove_if_last_and_not_all

  from arrays a
