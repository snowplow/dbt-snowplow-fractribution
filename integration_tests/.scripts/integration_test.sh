#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "databricks" "snowflake")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  echo "Snowplow Fractribution integration tests: Seeding data"

  eval "dbt seed --full-refresh --target $db" || exit 1;

  echo "Snowplow Fractribution integration tests: Execute events_stg for web package"

  eval "dbt run --select snowplow_fractribution_events_stg --full-refresh --target $db" || exit 1;

  echo "Snowplow Web: Execute models"

  eval "dbt run --select snowplow_web --full-refresh --vars '{snowplow__allow_refresh: true}' --target $db" || exit 1;

  echo "Snowplow Fractribution integration tests: Execute fractribution models"

  eval "dbt run --select snowplow_fractribution --full-refresh --target $db " || exit 1;

  echo "Snowplow Fractribution integration tests: Execute fractribution integration test models"

  eval "dbt run --select snowplow_fractribution_integration_tests --target $db --full-refresh" || exit 1;

  echo "Snowplow Fractribution integration tests: Test models"

  eval "dbt test --exclude snowplow_web --target $db" || exit 1;

  echo "Snowplow Fractribution integration tests: All tests passed"

done
