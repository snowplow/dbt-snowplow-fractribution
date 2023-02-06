{% docs macro_channel_classification %}

{% raw %}
A macro used to perform channel classifications. Each channel should be classified a name that is a valid field name as it will be used for that purpose, once unnested downstream.

#### Returns

A sql of case statements that determine which channel is classified (it is most likely unique to each organisation, the sample provided is based on Google's Fractribution).

#### Usage

```sql
    case when lower(mkt_medium) in ('cpc', 'ppc') and regexp_count(lower(mkt_campaign), 'brand') > 0 then 'Paid_Search_Brand'
         when lower(mkt_medium) in ('cpc', 'ppc') and regexp_count(lower(mkt_campaign), 'generic') > 0 then 'Paid_Search_Generic'
         when lower(mkt_medium) in ('cpc', 'ppc') and not regexp_count(lower(mkt_campaign), 'brand|generic') > 0 then 'Paid_Search_Other'
         when lower(mkt_medium) = 'organic' then 'Organic_Search'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and regexp_count(lower(mkt_campaign), 'prospect') > 0 then 'Display_Prospecting'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and regexp_count(lower(mkt_campaign), 'retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Display_Retargeting'
         when lower(mkt_medium) in ('display', 'cpm', 'banner') and not regexp_count(lower(mkt_campaign), 'prospect|retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Display_Other'
         when regexp_count(lower(mkt_campaign), 'video|youtube') > 0 or regexp_count(lower(mkt_source), 'video|youtube') > 0 then 'Video'
         when lower(mkt_medium) = 'social' and regexp_count(lower(mkt_campaign), 'prospect') > 0 then 'Paid_Social_Prospecting'
         when lower(mkt_medium) = 'social' and regexp_count(lower(mkt_campaign), 'retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Paid_Social_Retargeting'
         when lower(mkt_medium) = 'social' and not regexp_count(lower(mkt_campaign), 'prospect|retargeting|re-targeting|remarketing|re-marketing') > 0 then 'Paid_Social_Other'
         when mkt_source = '(direct)' then 'Direct'
         when lower(mkt_medium) = 'referral' then 'Referral'
         when lower(mkt_medium) = 'email' then 'Email'
         when lower(mkt_medium) in ('cpc', 'ppc', 'cpv', 'cpa', 'affiliates') then 'Other_Advertising'
         else 'Unmatched_Channel'
    end
```
{% endraw %}
{% enddocs %}

{% docs macro_channel_spend %}

{% raw %}
 A macro for the user to overwrite it with a sql script to extract total ad spend by channel.

#### Returns

A sql script to extract channel and corresponding spend values from a data source.


#### Usage

```sql

  -- Example (simplified) query:

  select
    channel,
    sum(spend_usd) as spend
  from example_spend_table
  group by 1

  -- Example table output for the user-supplied SQL:

  Channel     |  Spend
 ------------------------
  direct      |  1050.02
  paid_search |  10490.11
  etc...

```
{% endraw %}
{% enddocs %}


{% docs macro_conversion_clause %}

{% raw %}
A macro to let users specify how to filter on conversion events.

#### Returns

A sql to be used in a WHERE clause to filter on conversion events.

#### Usage

```sql
 tr_total > 0

```
{% endraw %}
{% enddocs %}


{% docs macro_conversion_value %}

{% raw %}
A user defined macro that specifies either a single column or a calculated value that represents the value associated with the conversion.

#### Returns

A sql to be used to refer to the conversion value.

#### Usage

```sql

tr_total

```
{% endraw %}
{% enddocs %}


{% docs macro_get_lookback_date_limits %}

{% raw %}
A macro returning the upper or lower boundary to limit what is processed by the sessions_by_customer_id model.

#### Returns

A string value of the upper or lower date limit.

#### Usage

A macro call with 'min' or 'max' given as a parameter.

```sql
select
  ...
from 
  ...
where 
  date(derived_tstamp) >= '{{ get_lookback_date_limits("min") }}'
  and date(derived_tstamp) <= '{{ get_lookback_date_limits("max") }}'
  
-- returns
select
  ...
from 
  ...
where 
  date(derived_tstamp) >= '2023-01-01 13:45:03'
  and date(derived_tstamp) <= '2023-02-01 10:32:52'
```
{% endraw %}
{% enddocs %}


{% docs macro_create_udfs %}

{% raw %}
Creates user defined functions for adapters apart from Databricks. It is executed as part of an on-start hook.

#### Returns

Nothing, sql is executed which creates the UDFs in the target database and schema.

#### Usage

```yml
-- dbt_project.yml
...
on-run-start: "{{ create_udfs() }}"
...

```
{% endraw %}
{% enddocs %}


{% docs macro_path_transformation %}

{% raw %}
 Macro to execute the indvidual path_transformation specified as a parameter.

#### Returns

The transformed array column.


#### Usage

```sql

```
{% endraw %}
{% enddocs %}


{% docs macro_transform_paths %}

{% raw %}
Macro to remove complexity from models paths_to_conversion / paths_to_non_conversion.

#### Returns

The sql with the missing cte's that take care of path transformations.

#### Usage

It is used by the transform_paths() macro for the transformation cte sql code build. It takes a transformation type as a parameter and its optional argument, if exists. The E.g.

```sql
with base_data as (...),

{{ transform_paths('conversions', 'base_data') }}
 
select * from path_transforms
```

{% endraw %}
{% enddocs %}


{% docs macro_trim_long_path %}

{% raw %}
Returns the last 'path_lookback_steps' number of channels in the path if path_lookback_steps > 0, or the full path otherwise.

#### Returns

The transformed array column.


#### Usage

```sql

select
  ...
  {{ trim_long_path('path', var('path_lookback_steps')) }} as path,
  ...
from 
  ...

```
{% endraw %}
{% enddocs %}


