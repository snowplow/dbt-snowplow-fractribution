
/* these test cases should help understand how the upper and lower limits work for developers
   integration test data when processed by the web model results in '2022-06-03 04:44:32.000' as first_pageview, '2022-08-01 05:37:27.000' as last_pageview
   illustration: user would like to cover the conversion window between '2022-07-03' and '2022-07-31' (both are inclusive), this covers 29 days
   using the default 30 days lookback period we should process pageview data from '2022-06-03' */

{% set combined_time = 29 + 30 %}

    with case1 as
        (with base as (
            select max(start_tstamp) as last_pageview
            from {{target.schema}}_derived.snowplow_web_page_views
         )
        select cast({{ dbt.dateadd('day', -combined_time, 'last_pageview') }} as date) as result, 'lower_limit' as limit_type, 'sessions' as model_type, 'auto' as update_type, 'case1' as test_case_number
        from base
    )

    , case2 as (

        with base as (
        select cast('2022-07-03' as timestamp) as cw_tstamp
        )
        select cast({{ dbt.dateadd('day', -30, 'cw_tstamp' ) }} as date) as result, 'lower_limit' as limit_type, 'sessions' as model_type, 'manual' as update_type, 'case2' as test_case_number
        from base
    )

    , case3 as (
      with base as (
                select max(start_tstamp) as last_pageview
                from {{target.schema}}_derived.snowplow_web_page_views
            )
        select cast({{ dbt.dateadd('day', -1, 'last_pageview') }} as date) as result, 'upper_limit' as limit_type, 'sessions' as model_type, 'auto' as update_type, 'case3' as test_case_number
        from base

    )

    , case4 as (

    select cast('2022-07-31' as date) as result, 'upper_limit' as limit_type, 'sessions' as model_type, 'manual' as update_type, 'case4' as test_case_number
    )

    , case5 as (
          with base as (
            select max(start_tstamp) as last_pageview
            from {{target.schema}}_derived.snowplow_web_page_views
          )
        select cast( {{ dbt.dateadd('day', -29, 'last_pageview') }} as date) as result, 'lower_limit' as limit_type, 'conversions' as model_type, 'auto' as update_type, 'case5' as test_case_number
        from base
    )

    , case6 as (

    select cast('2022-07-03' as date) as result, 'lower_limit' as limit_type, 'conversions' as model_type, 'manual' as update_type, 'case6' as test_case_number
    )

    , case7 as (

        with base as (
                select max(start_tstamp) as last_pageview
                from {{target.schema}}_derived.snowplow_web_page_views
            )
        select cast({{ dbt.dateadd('day', -1, 'last_pageview') }} as date) as result, 'upper_limit' as limit_type, 'conversions' as model_type, 'auto' as update_type, 'case7' as test_case_number
        from base
    )

    , case8 as (

        select cast('2022-07-31' as date) as result, 'upper_limit' as limit_type, 'conversions' as model_type, 'manual' as update_type, 'case8' as test_case_number
    )

    select * from case1
    union all
    select * from case2
    union all
    select * from case3
    union all
    select * from case4
    union all
    select * from case5
    union all
    select * from case6
    union all
    select * from case7
    union all
    select * from case8
