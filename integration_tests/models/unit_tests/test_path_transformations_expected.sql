with expected_result as (

  select
    'Example, Video, Direct, Direct' as raw_array,
    'Direct' as trim_long_path,
    'Direct, Direct' as trim_long_path2,
    'Example, Video, Direct, Direct' as unique_path,
    'Example(1), Video(1), Direct(2)' as frequency_path,
    'Example, Video, Direct' as exposure_path,
    'Example, Video, Direct' as first_path,
    'Example, Video' as remove_if_not_all,
    'Example, Video' as remove_if_last_and_not_all

  union all

  select
    'Direct, Direct' as raw_array,
    'Direct' as trim_long_path,
    'Direct, Direct' as trim_long_path2,
    'Direct, Direct' as unique_path,
    'Direct(2)' as frequency_path,
    'Direct' as exposure_path,
    'Direct' as first_path,
    'Direct, Direct' as remove_if_not_all,
    'Direct, Direct' as remove_if_last_and_not_all

  union all

  select
    'a, a, a, Direct, a, Direct, Direct' as raw_array,
    'Direct' as trim_long_path,
    'Direct, Direct' as trim_long_path2,
    'a, a, a, Direct, a, Direct, Direct' as unique_path,
    'a(4), Direct(3)' as frequency_path,
    'a, Direct, a, Direct' as exposure_path,
    'a, Direct' as first_path,
    'a, a, a, a' as remove_if_not_all,
    'a, a, a, Direct, a' as remove_if_last_and_not_all

  union all

  select
    'Direct' as raw_array,
    'Direct' as trim_long_path,
    'Direct' as trim_long_path2,
    'Direct' as unique_path,
    'Direct(1)' as frequency_path,
    'Direct' as exposure_path,
    'Direct' as first_path,
    'Direct' as remove_if_not_all,
    'Direct' as remove_if_last_and_not_all

  union all

  select
    '' as raw_array,
    '' as trim_long_path,
    '' as trim_long_path2,
    '' as unique_path,
    '(1)' as frequency_path,
    '' as exposure_path,
    '' as first_path,
    '' as remove_if_not_all,
    '' as remove_if_last_and_not_all

  union all

  select
    'Example, Video, Direct' as raw_array,
    'Direct' as trim_long_path,
    'Video, Direct' as trim_long_path2,
    'Example, Video, Direct' as unique_path,
    'Example(1), Video(1), Direct(1)' as frequency_path,
    'Example, Video, Direct' as exposure_path,
    'Example, Video, Direct' as first_path,
    'Example, Video' as remove_if_not_all,
    'Example, Video' as remove_if_last_and_not_all

)

, arrays as (

  select
    {{ snowplow_utils.get_split_to_array('raw_array', 'e', ', ') }} as raw_array,
    {{ snowplow_utils.get_split_to_array('trim_long_path', 'e', ', ') }} as trim_long_path,
    {{ snowplow_utils.get_split_to_array('trim_long_path2', 'e', ', ') }} as trim_long_path2,
    {{ snowplow_utils.get_split_to_array('unique_path', 'e', ', ') }} as unique_path,
    {{ snowplow_utils.get_split_to_array('frequency_path', 'e', ', ') }} as frequency_path,
    {{ snowplow_utils.get_split_to_array('exposure_path', 'e', ', ') }} as exposure_path,
    {{ snowplow_utils.get_split_to_array('first_path', 'e', ', ') }} as first_path,
    {{ snowplow_utils.get_split_to_array('remove_if_not_all', 'e', ', ') }} as remove_if_not_all,
    {{ snowplow_utils.get_split_to_array('remove_if_last_and_not_all', 'e', ', ') }} as remove_if_last_and_not_all

  from expected_result e
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
