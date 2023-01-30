# snowplow-fractribution-integration-tests

Integration test suite for the snowplow-fractribution dbt package.

The `./scripts` directory contains two scripts:

- `integration_tests.sh`: This tests the standard modules of the snowplow-fractribution package. It runs the Snowplow web package to generate the derived.snowplow_we_page_views as a source then runs the package with the basic setup.

Run the scripts using:

```bash
bash integration_tests.sh -d {warehouse}
```

Supported warehouses:

- snowflake
- databricks
