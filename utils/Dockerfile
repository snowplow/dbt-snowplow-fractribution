FROM python:3.8-slim
USER root

ENV snowflake_account=${snowflake_account}
ENV snowflake_user=${snowflake_user}
ENV snowflake_password=${snowflake_password}
ENV snowflake_user_role=${snowflake_user_role}
ENV snowflake_warehouse=${snowflake_warehouse}
ENV snowflake_database=${snowflake_database}
ENV snowflake_schema=${snowflake_schema}
ENV attribution_model=${attribution_model:-shapley}
ENV conversion_window_start_date=${conversion_window_start_date}
ENV conversion_window_end_date=${conversion_window_end_date}

WORKDIR /app

COPY fractribution.py .
COPY main_snowplow_snowflake.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["sh", "-c", "python main_snowplow_snowflake.py --attribution_model=$attribution_model --conversion_window_start_date=$conversion_window_start_date --conversion_window_end_date=$conversion_window_end_date"]