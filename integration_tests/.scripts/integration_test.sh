#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("databricks"  "snowflake")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  echo "Snowplow Fractribution integration tests: Seeding data"

  eval "dbt seed --target $db --full-refresh" || exit 1;

  echo "Snowplow Fractribution integration tests: Execute events_stg for web package"

  eval "dbt run --select snowplow_fractribution_events_stg --target $db --full-refresh" || exit 1;

  echo "Snowplow Web: Execute models"

  eval "dbt run --select snowplow_web --target $db --full-refresh --vars '{snowplow__allow_refresh: true}'" || exit 1;

  echo "Snowplow Fractribution integration tests: Execute fractribution models"

  eval "dbt run --select snowplow_fractribution --target $db --full-refresh" || exit 1;

  echo "Snowplow Fractribution integration tests: Execute fractribution integration test models"

  eval "dbt run --select snowplow_fractribution_integration_tests --target $db --full-refresh" || exit 1;

  echo "Snowplow Fractribution integration tests: Test models"

  eval "dbt test --target $db --exclude snowplow_web" || exit 1;

  echo "Snowplow Fractribution integration tests: All tests passed"

done
