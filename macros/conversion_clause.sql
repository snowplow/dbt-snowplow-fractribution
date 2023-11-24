{#
Copyright (c) 2022-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

/* Define a conditional (where clause) that filters to conversion events
   by default we use tr_total but this may refer to an entity
   or event (e.g., event_name = 'checkout') */

{% macro conversion_clause() %}
    {{ return(adapter.dispatch('conversion_clause', 'snowplow_fractribution')()) }}
{% endmacro %}

{% macro default__conversion_clause() %}
    tr_total > 0
{% endmacro %}
