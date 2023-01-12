with data as (

  select 'Example, Video, Direct, Direct' as path

  union all

  select 'Direct, Direct' as path

  union all

  select 'a, a, a, Direct, a, Direct, Direct'

    union all

  select 'Direct'

  union all

  select '' as path

  union all

  select 'Example, Video, Direct' as path

  union all

  select 'Example, Video, ' as path

)

, make_it_array as (

  select
     {{ snowplow_utils.get_split_to_array('path', 'd', ', ') }} as transformed_path

  from data d
)



select

 *

from make_it_array
