{% if target.type == 'snowflake'%}
select *

from {{ source('python_created_tables', 'snowplow_fractribution_channel_attribution') }}
-- Using source() here to avoid a node error when the table is not found in the models/ folder (as it is created by the python script, not dbt). 
{% else %}
-- non-Snowflake targets just need a dummy select as no tests will be run on these models
select 1 as col
{% endif %}