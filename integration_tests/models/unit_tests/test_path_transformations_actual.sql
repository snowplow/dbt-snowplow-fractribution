
with raw_data as (

    select * from {{ ref('test_path_transformations_data') }}

  )

  select
    transformed_path as raw_array,
    {{ snowplow_fractribution.trim_long_path('transformed_path', 1) }} as trim_long_path,
    {{ snowplow_fractribution.path_transformation('unique_path') }} as unique_path,
    {{ snowplow_fractribution.path_transformation('frequency_path', '', 'd') }} as frequency_path,
    {{ snowplow_fractribution.path_transformation('exposure_path', '', 'd') }} as exposure_path,
    {{ snowplow_fractribution.path_transformation('first_path') }} as first_path,
    {{ snowplow_fractribution.path_transformation('remove_if_not_all', 'Direct') }} as remove_if_not_all,
    {{ snowplow_fractribution.path_transformation('remove_if_last_and_not_all', 'Direct', 'd') }} as remove_if_last_and_not_all

  from raw_data d
