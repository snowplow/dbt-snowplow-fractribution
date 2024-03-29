FROM python:3.8-slim
USER root

ENV attribution_model=${attribution_model:-shapley}
ENV conversion_window_start_date=${conversion_window_start_date}
ENV conversion_window_end_date=${conversion_window_end_date}
ENV warehouse=${warehouse}

# Snowflake-specific env variables
ENV snowflake_account=${snowflake_account}
ENV snowflake_user=${snowflake_user}
ENV snowflake_password=${snowflake_password}
ENV snowflake_user_role=${snowflake_user_role}
ENV snowflake_warehouse=${snowflake_warehouse}
ENV snowflake_database=${snowflake_database}
ENV snowflake_schema=${snowflake_schema}

# Bigquery-specific env variables
ENV project_id=${project_id}
ENV bigquery_dataset=${bigquery_dataset}
ENV google_application_credentials=/keyfile.json

# Databricks-specific env variables
ENV databricks_schema=${databricks_schema}
ENV databricks_server_hostname=${databricks_server_hostname}
ENV databricks_http_path=${databricks_http_path}
ENV databricks_token=${databricks_token}

# Redshift-specific env variables
ENV redshift_host=${redshift_host}
ENV redshift_database=${redshift_database}
ENV redshift_port=${redshift_port}
ENV redshift_user=${redshift_user}
ENV redshift_password=${redshift_password}
ENV redshift_schema=${redshift_schema}

# Conditional --verbose tag, set to "true", if needed for debugging
ARG verbose_run
ENV verbose_run=${verbose_run}

WORKDIR /app

COPY fractribution.py .
COPY main_snowplow_bigquery.py .
COPY main_snowplow_databricks.py .
COPY main_snowplow_snowflake.py .
COPY main_snowplow_redshift.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["sh", "-c",  "if [ \"$verbose_run\" = \"true\" ]; then python main_snowplow_$warehouse.py --attribution_model=$attribution_model --conversion_window_start_date=$conversion_window_start_date --conversion_window_end_date=$conversion_window_end_date --verbose; else python main_snowplow_$warehouse.py --attribution_model=$attribution_model --conversion_window_start_date=$conversion_window_start_date --conversion_window_end_date=$conversion_window_end_date; fi"]
