/* User supplied SQL script to extract total ad spend by channel, by replacing the SQL query in the default__channel_spend() macro.
   Required output schema:
   channel: STRING not NULL
   spend: FLOAT64 (Use the same monetary units as conversion revenue, and NULL if unknown.)
   Note that all flags are passed into this template (e.g. conversion_window_start/end_date).

  Example (simplified) query:

  select
    channel,
    sum(spend_usd) as spend
  from example_spend_table
  group by 1

  Example table output for the user-supplied SQL:

  Channel     |  Spend
 ------------------------
  direct      |  1050.02
  paid_search |  10490.11
  etc... */


{% macro channel_spend() %}
    {{ return(adapter.dispatch('channel_spend', 'snowplow_fractribution')()) }}
{% endmacro %}


{% macro default__channel_spend() %}

  with channels as (

      select
        1 as id,
        array_agg(distinct cast(channel as {{ dbt.type_string() }})) as c

      from {{ ref('snowplow_fractribution_channel_counts') }}
  )

  , unnesting as (

      {{ snowplow_utils.unnest('id', 'c', 'channel', 'channels') }}
  )

  select
    channel,
    10000 as spend

  from unnesting

{% endmacro %}
