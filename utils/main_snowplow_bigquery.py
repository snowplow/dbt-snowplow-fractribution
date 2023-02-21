# coding=utf-8
# Copyright 2022-2023 Snowplow, Google LLC..
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Loads the data needed to run Fractribution into Bigquery.
It produces the following output tables in the data warehouse:

snowplow_fractribution_path_summary_with_channels
snowplow_fractribution_report_table
snowplow_fractribution_channel_attribution"""

import os
import re
from typing import Any, Dict, List, Mapping, Optional, Tuple
from absl import app
from absl.flags import argparse_flags
import fractribution
from google.cloud import bigquery

# Construct a BigQuery client object with authentication from JSON
SERVICE_ACCOUNT_JSON = os.environ["google_application_credentials"]
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

project_id = os.environ.get('project_id')
dataset = os.environ.get('bigquery_dataset')

VALID_CHANNEL_NAME_PATTERN = re.compile(r"^[a-zA-Z_]\w+$", re.ASCII)

def _is_valid_column_name(column_name: str) -> bool:
    """Returns True if the column_name is a valid Bigquery column name."""

    return (
        len(column_name) <= 300
        and VALID_CHANNEL_NAME_PATTERN.match(column_name) is not None
    )


def _extract_channels(client) -> List[str]:
    """Returns the list of names by running extract_channels.sql.

    Args:
      client: Client.
      params: Mapping of template parameter names to values.
    Returns:
      List of channel names.
    Raises:
      ValueError: User-formatted error if channel is not a valid Bigquery column.
    """

    channels = [row.channel for row in client]
    for channel in channels:
        if not _is_valid_column_name(channel):
            raise ValueError("Channel is not a legal Bigquery column name: ", channel)
    return channels


def parse_args(argv):
    ap = argparse_flags.ArgumentParser()
    model = ap.add_argument_group(title="Attribution model")
    model.add_argument(
        "--attribution_model",
        type=str,
        help="Attribution model. One of: 'shapley', 'first_touch', 'last_touch', 'position_based', 'linear'",
        default="shapley",
    )

    window = ap.add_argument_group(title="Conversion window")
    window.add_argument(
        "--conversion_window_start_date",
        type=str,
        required=True,
        help="Start date of the window for conversions",
    )
    window.add_argument(
        "--conversion_window_end_date",
        type=str,
        required=True,
        help="End date of the window for conversions",
    )

    tool_helper = ap.add_argument_group(title="Helper")
    tool_helper.add_argument(
        "--verbose",
        help="Increase output verbosity",
        action="store_true"
    )


    args = ap.parse_args(argv[1:])
    if args.attribution_model not in fractribution.Fractribution.ATTRIBUTION_MODELS:
        raise ValueError(
            f"Unknown attribution_model. Use one of: {list(fractribution.Fractribution.ATTRIBUTION_MODELS.keys())}"
        )


    return args


def get_channels():
    """Enumerates all possible channels."""
    query = f"""SELECT DISTINCT channel FROM `{project_id}.{dataset}.snowplow_fractribution_channel_counts`"""
    channels = client.query(query)
    return channels


def get_path_summary_data():
    query = f"""
        SELECT transformed_path, CAST(conversions AS FLOAT64) AS conversions, CAST(non_conversions AS FLOAT64) AS non_conversions, CAST(revenue AS FLOAT64) AS revenue
        FROM `{project_id}.{dataset}.snowplow_fractribution_path_summary`
        """
    path_summary_data = client.query(query)
    # df = path_summary_data.to_dataframe()

    return path_summary_data


def create_attribution_report_table():
    query = f"""
        CREATE OR REPLACE TABLE {project_id}.{dataset}.snowplow_fractribution_report_table AS
        SELECT
            *,
            IFNULL(SAFE_DIVIDE(revenue, CAST(spend as float64)), 0) AS roas
        FROM
            {project_id}.{dataset}.snowplow_fractribution_channel_attribution
            LEFT JOIN
            {project_id}.{dataset}.snowplow_fractribution_channel_spend USING (channel)
    """

    return client.query(query)


def run_fractribution(params: Mapping[str, Any]) -> None:
    """Runs fractribution on the Bigquery tables.

    Args:
      params: Mapping of all template parameter names to values.
    """

    path_summary = get_path_summary_data()

    # Step 1: Extract the paths from the path_summary_table.
    frac = fractribution.Fractribution(path_summary)
    frac.run_fractribution(params["attribution_model"])
    frac.normalize_channel_to_attribution_names()
    path_list = frac._path_summary_to_list()
    types = [
                bigquery.SchemaField("revenue", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("conversions", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("non_conversions", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("transformed_path", "STRING", mode="NULLABLE"),
            ]

    # exclude revenue, conversions, non_conversions, transformed_path
    channel_to_attribution = frac._get_channel_to_attribution()
    un = set(channel_to_attribution.keys()).difference(["revenue", "conversions", "non_conversions", "transformed_path"])
    attribution_types = [bigquery.SchemaField(k, "FLOAT64", mode="NULLABLE") for k in list(un)]
    schema = types + attribution_types

    paths_table = bigquery.Table(f"{project_id}.{dataset}.snowplow_fractribution_path_summary_with_channels", schema)
    table = client.create_table(paths_table, exists_ok=True)

    # Load data into BigQuery
    job_config=bigquery.LoadJobConfig()
    job_config.write_disposition="WRITE_TRUNCATE"
    client.load_table_from_json(path_list, table, job_config)

    conversion_window_start_date = params["conversion_window_start_date"]
    conversion_window_end_date = params["conversion_window_end_date"]

    channel_to_attribution = frac._get_channel_to_attribution()

    channel_to_revenue = frac._get_channel_to_revenue()
    rows = []
    for channel, attribution in channel_to_attribution.items():
        row = {
            "conversion_window_start_date": conversion_window_start_date,
            "conversion_window_end_date": conversion_window_end_date,
            "channel": channel,
            "conversions": attribution,
            "revenue": channel_to_revenue.get(channel, 0.0),
        }
        rows.append(row)

    schema=[
                bigquery.SchemaField("conversion_window_start_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("conversion_window_end_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("channel", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("conversions", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("revenue", "FLOAT64", mode="NULLABLE"),
            ]

    channel_attribution_table = bigquery.Table(f"{project_id}.{dataset}.snowplow_fractribution_channel_attribution", schema)
    table = client.create_table(channel_attribution_table, exists_ok=True)
    if params["verbose"]:
        print("Table snowplow_fractribution_path_summary_with_channels is created. Inserting data...")

    # Load data into BigQuery
    jc=bigquery.LoadJobConfig()
    jc.write_disposition="WRITE_TRUNCATE"
    client.load_table_from_json(rows, table, job_config=jc)
    if params["verbose"]:
        print("Uploading data to snowplow_fractribution_path_summary_with_channels finished.")

    report = create_attribution_report_table()



def run(input_params: Mapping[str, Any]) -> int:
    """Main entry point to run Fractribution with the given input_params.

    Args:
      input_params: Mapping from input parameter names to values.
    Returns:
      0 on success and non-zero otherwise
    """
    params = input_params

    # assumes that the dataset already exists
    params["channel_counts_table"] = "snowplow_fractribution_channel_counts"

    channels = get_channels()

    params["channels"] = _extract_channels(channels, None)

    run_fractribution(params)

    return 0

def standalone_main(args):
    input_params = {
        "attribution_model": args.attribution_model,
        "conversion_window_start_date": args.conversion_window_start_date,
        "conversion_window_end_date": args.conversion_window_end_date,
        "verbose": args.verbose,
    }
    run(input_params)
    print("Report table created")


if __name__ == "__main__":
    app.run(standalone_main, flags_parser=parse_args)
