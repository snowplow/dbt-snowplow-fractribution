{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

WITH PathsToConversion AS (
  SELECT transformed_path, COUNT(*) AS conversions, SUM(revenue) AS revenue
  FROM
    {{ ref('snowplow_fractribution_paths_to_conversion') }}
  GROUP BY transformed_path
), PathsToNonConversion AS (
  SELECT transformed_path, COUNT(*) AS non_conversions
  FROM
    {{ ref('snowplow_fractribution_paths_to_non_conversion') }}
    GROUP BY transformed_path
)
SELECT
  IFNULL(PathsToConversion.transformed_path,
         PathsToNonConversion.transformed_path) AS transformed_path,
  IFNULL(PathsToConversion.conversions, 0) AS conversions,
  IFNULL(PathsToNonConversion.non_conversions, 0) AS non_conversions,
  PathsToConversion.revenue
FROM PathsToConversion
FULL JOIN PathsToNonConversion
  USING(transformed_path)
