{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

select cast('2022-06-03' as date) as result, 'lower_limit' as limit_type, 'sessions' as model_type, 'auto' as update_type, 'case1' as test_case_number
union all
select cast('2022-06-03' as date) as result, 'lower_limit' as limit_type, 'sessions' as model_type, 'manual' as update_type, 'case2' as test_case_number
union all
select cast('2022-07-31' as date) as result, 'upper_limit' as limit_type, 'sessions' as model_type, 'auto' as update_type, 'case3' as test_case_number
union all
select cast('2022-07-31' as date) as result, 'upper_limit' as limit_type, 'sessions' as model_type, 'manual' as update_type, 'case4' as test_case_number
union all
select cast('2022-07-03' as date) as result, 'lower_limit' as limit_type, 'conversions' as model_type, 'auto' as update_type, 'case5' as test_case_number
union all
select cast('2022-07-03' as date) as result, 'lower_limit' as limit_type, 'conversions' as model_type, 'manual' as update_type, 'case6' as test_case_number
union all
select cast('2022-07-31' as date) as result, 'upper_limit' as limit_type, 'conversions' as model_type, 'auto' as update_type, 'case7' as test_case_number
union all
select cast('2022-07-31' as date) as result, 'upper_limit' as limit_type, 'conversions' as model_type, 'manual' as update_type, 'case8' as test_case_number
