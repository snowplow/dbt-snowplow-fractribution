WITH PathsToConversion AS (
  SELECT transformedPath, COUNT(*) AS conversions, SUM(revenue) AS revenue
  FROM
    {{ ref('paths_to_conversion') }}
  GROUP BY transformedPath
), PathsToNonConversion AS (
  SELECT transformedPath, COUNT(*) AS nonConversions
  FROM 
    {{ ref('paths_to_non_conversion') }}
    GROUP BY transformedPath
)
SELECT
  IFNULL(PathsToConversion.transformedPath,
         PathsToNonConversion.transformedPath) AS transformedPath,
  IFNULL(PathsToConversion.conversions, 0) AS conversions,
  IFNULL(PathsToNonConversion.nonConversions, 0) AS nonConversions,
  PathsToConversion.revenue
FROM PathsToConversion
FULL JOIN PathsToNonConversion
  USING(transformedPath)
