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

"""Loads the data needed to run Fractribution into Redshift.
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
import pandas as pd
from collections import namedtuple
import redshift_connector as sql

VALID_CHANNEL_NAME_PATTERN = re.compile(r"^[a-zA-Z_]\w+$", re.ASCII)

db_schema = os.getenv("redshift_schema")


def connect_to_redshift():

    cnx = sql.connect(host=os.getenv("redshift_host"),
                      database=os.getenv("redshift_database"),
                      port=int(os.getenv("redshift_port")),
                      user=os.getenv("redshift_user"),
                      password=os.getenv("redshift_password")
                      )

    cnx.autocommit = True
    cs = cnx.cursor()
    print("Connected to Redshift")


    return cs, cnx


def fetch_results_as_pandas_dataframe(cs, query, column_names):
    cs.execute(query)
    rows = cs.fetchall()
    names = [x[0] for x in column_names]

    return pd.DataFrame(rows, columns=names)


def _is_valid_column_name(column_name: str) -> bool:
    """Returns True if the column_name is a valid redshift column name."""

    return (
        len(column_name) <= 115
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
      ValueError: User-formatted error if channel is not a valid Redshift column.
    """

    channels = [row for row in client["c"]]
    for channel in channels:
        if not _is_valid_column_name(channel):
            raise ValueError(
                "Channel is not a legal Redshift column name: ", channel)
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


def get_channels(cs):
    """Enumerates all possible channels."""
    query = f"SELECT DISTINCT channel FROM {db_schema}.snowplow_fractribution_channel_counts"

    column_names = ['channel']

    return fetch_results_as_pandas_dataframe(cs, query, column_names)


def get_path_summary_data(cs):
    query = f"SELECT transformed_path, CAST(conversions AS FLOAT) AS conversions, CAST(non_conversions AS float) AS non_conversions, CAST(revenue AS float) AS revenue FROM {db_schema}.snowplow_fractribution_path_summary"
    column_names = ['transformed_path',
                    'conversions', 'non_conversions', 'revenue']

    df = fetch_results_as_pandas_dataframe(cs, query, column_names)
    df = df.fillna("NULL")

    # change pandas dataframe into a list of tuples for further processing (in line with Snowpark output)
    snowpark_df_base = list(df.itertuples(index=False, name=None))
    tup = ("TRANSFORMED_PATH", "CONVERSIONS", "NON_CONVERSIONS", "REVENUE")
    Row = namedtuple("Row",  tup)
    snowpark_df = []
    for i in snowpark_df_base:
        snowpark_df.append(Row._make(i))

    return snowpark_df


def create_attribution_report_table(cs):
    query = f"""
        CREATE TABLE {db_schema}.snowplow_fractribution_report_table AS
        SELECT
            *,
            coalesce(revenue/nullif(spend, 0), 0) AS roas
        FROM
            {db_schema}.snowplow_fractribution_channel_attribution a
        LEFT JOIN
            {db_schema}.snowplow_fractribution_channel_spend b USING (channel)
    """

    column_names = ['conversionwindowstartdate', 'conversionwindowenddate',
                    'channel', 'conversions', 'revenue', 'roas']
    cs.execute(f"DROP TABLE IF EXISTS {db_schema}.snowplow_fractribution_report_table")
    cs.execute(query)
    return fetch_results_as_pandas_dataframe(cs, f"SELECT * FROM {db_schema}.snowplow_fractribution_report_table", column_names)


def run_fractribution(params: Mapping[str, Any]) -> None:
    """Runs fractribution on the Redshift tables.

    Args:
      params: Mapping of all template parameter names to values.
    """

    try:
        cs, cnx = connect_to_redshift()
        path_summary = get_path_summary_data(cs)

        # Extract the paths from the path_summary_table.
        frac = fractribution.Fractribution(path_summary)
        frac.run_fractribution(params["attribution_model"])
        frac.normalize_channel_to_attribution_names()
        path_list = frac._path_summary_to_list()
        types = [{"revenue": "float"}, {"conversions": "float"}, {
            "non_conversions": "float"}, {"transformed_path": "varchar(max)"}]

        # Exclude revenue, conversions, non_conversions, transformed_path
        channel_to_attribution = frac._get_channel_to_attribution()
        un = set(channel_to_attribution.keys()).difference(
            ["revenue", "conversions", "non_conversions", "transformed_path"])

        attribution_types = [{k: "float"} for k in list(un)]
        schema = types + attribution_types

        # Get column names to then create the base table with the specified data types above
        columns = []
        column_types = []
        for dic in schema:
            for key, val in dic.items():
                columns.append(f"{key}")
                column_types.append(f"{key} {val}")

        text_column_types = ", ".join(column_types)
        text_columns = ", ".join(columns)
        cs.execute(f"DROP TABLE IF EXISTS {db_schema}.snowplow_fractribution_path_summary_with_channels")
        cs.execute(
            f"CREATE TABLE {db_schema}.snowplow_fractribution_path_summary_with_channels ({text_column_types})")
        if params["verbose"]:
            print(
                "Table snowplow_fractribution_path_summary_with_channels is created. Inserting data...")

        # Insert rows one at a time
        for dic in path_list:
            sql = f"INSERT INTO {db_schema}.snowplow_fractribution_path_summary_with_channels ({text_columns}) values("
            values = []
            for col in columns:
                if col == 'transformed_path':
                    values.append("\'"+str(dic[col])+"\'")
                else:
                    try:
                        values.append(str(dic[col]))
                    except BaseException as error:
                        values.append("NULL")
            sql += ", ".join(values) + ")"
            cs.execute(sql)

        if params["verbose"]:
            print(
                f"Uploading data to {db_schema}.snowplow_fractribution_path_summary_with_channels finished.")

        conversion_window_start_date = params["conversion_window_start_date"]
        conversion_window_end_date = params["conversion_window_end_date"]

        channel_to_attribution = frac._get_channel_to_attribution()
        channel_to_revenue = frac._get_channel_to_revenue()

        # Create and populate table snowplow_fractribution_channel_attribution
        cs.execute(f"DROP TABLE IF EXISTS {db_schema}.snowplow_fractribution_channel_attribution")
        cs.execute(
            f"CREATE TABLE {db_schema}.snowplow_fractribution_channel_attribution (conversion_window_start_date varchar(max), conversion_window_end_date  varchar(max), channel  varchar(max), conversions decimal(10, 2), revenue decimal(10, 2))")

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

        columns = ['conversion_window_start_date',
                   'conversion_window_end_date', 'channel', 'conversions', 'revenue']
        text_columns = ", ".join(columns)

        for dic in rows:
            sql = f"INSERT INTO {db_schema}.snowplow_fractribution_channel_attribution ({text_columns}) values("
            values = []
            for col in columns:
                if col == 'conversions' or col == 'revenue':
                    values.append(str(dic[col]))
                else:
                    values.append("\'"+str(dic[col])+"\'")
            sql += ", ".join(values) + ")"
            cs.execute(sql)

        report = create_attribution_report_table(cs)

    except BaseException as error:
        print('An exception occurred: {}'.format(error))
    finally:
        cs.close()
        cnx.close()


def run(input_params: Mapping[str, Any]) -> int:
    """Main entry point to run Fractribution with the given input_params.

    Args:
      input_params: Mapping from input parameter names to values.
    Returns:
      0 on success and non-zero otherwise
    """

    cs, cnx = connect_to_redshift()

    params = input_params

    # assumes that the dataset already exists
    params["channel_counts_table"] = "snowplow_fractribution_channel_counts"

    channels = get_channels(cs)

    cs.close()
    cnx.close()

    params["channels"] = _extract_channels(channels)

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
