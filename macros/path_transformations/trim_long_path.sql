{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

/* Returns the last 'snowplow__path_lookback_steps' number of channels in the path if snowplow__path_lookback_steps > 0,
   or the full path otherwise. */

{% macro trim_long_path(array_column, lookback_steps=var('snowplow__path_lookback_steps')) %}
  {{ return(adapter.dispatch('trim_long_path', 'snowplow_fractribution')(array_column,lookback_steps)) }}
{% endmacro %}

{% macro default__trim_long_path(array_column, lookback_steps=var('snowplow__path_lookback_steps')) %}

  {{ target.schema }}.trim_long_path({{ array_column }}, {{ lookback_steps }})

{% endmacro %}

{% macro spark__trim_long_path(array_column, lookback_steps=var('snowplow__path_lookback_steps')) %}

  case when array_size({{ array_column }}) <= {{ lookback_steps }} then {{ array_column }}
  when {{ lookback_steps }} == 0 then {{ array_column }}
  else slice({{ array_column }}, (-cast( {{lookback_steps }} as int)), (cast({{ lookback_steps }} as int)))
  end

{% endmacro %}
