[![early-release]][tracker-classification]
[![Release][release-image]][releases]
[![License][license-image]][license]
[![Discourse posts][discourse-image]][discourse]

![snowplow-logo](https://raw.githubusercontent.com/snowplow/dbt-snowplow-utils/main/assets/snowplow_logo.png)

# snowplow-fractribution

This dbt package:
- Uses page view and conversion events to perform Attribution Modelling on your Snowplow data
- Is used in conjunction with a Python script or Docker image to create the final output table
- Is designed to be customized, allowing you to easily make modifications to suit your data and objectives

Please refer to the [doc site](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-models/dbt-fractribution-data-model/) for a full breakdown of the package.

### Getting Started

The easiest way to get started is to follow our [QuickStart guide](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-quickstart/fractribution/), or to use our [Advanced Analytics for Web Accelerator](https://docs.snowplow.io/accelerators/snowplow-fractribution/) which includes steps for setting up tracking as well as modeling.

### Adapter Support

The latest version of the snowplow-fractribution package supports Snowflake, Databricks and BigQuery. For previous versions see our [package docs](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/).

### Requirements

- A dataset of web events from the [Snowplow Javascript Tracker](https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/) and familiarity with the [snowplow-web](https://hub.getdbt.com/snowplow/snowplow_web/latest/) dbt package
- The `snowplow_web_page_views` table generated by the snowplow-web package available in your warehouse
- A `conversions` dataset available in your warehouse (a dataset containing revenue data by users).
- A `channel spend` dataset available in your warehouse (a dataset containing total spend by channel)

### Installation

Check [dbt Hub](https://hub.getdbt.com/snowplow/snowplow_fractribution/latest/) for the latest installation instructions.
### Configuration & Operation

Please refer to the [doc site](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/) for details on how to [configure](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-configuration/fractribution/) and [run](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-quickstart/fractribution/) the package.

### Models

The package contains multiple models that are used by the Python script for the final attribution calculation:

| Model                     | Description                                                                           |
| ------------------------- | ------------------------------------------------------------------------------------- |
| snowplow_fractribution_channel_counts   | A count of sessions per channel, campaign, source and medium|
| snowplow_fractribution_channel_spend | The amount spent on advertising for each channel |
| snowplow_fractribution_conversions_by_customer_id | Each conversion and associated revenue per customer_id|
| snowplow_fractribution_path_summary | For each unique path, a summary of associated conversions, non conversions and revenue |
| snowplow_fractribution_paths_to_conversion | Customer id and the the paths the customer has followed that have lead to conversion|
| snowplow_fractribution_paths_to_non_conversion | Customer id and the the paths the customer has followed that have not lead to conversion|
| snowplow_fractribution_sessions_by_customer_id | Channels per session by customer id|

The tables produced by the Python script are the following:

| Table                                      | Description                                                                           |
| ------------------------------------------ | ------------------------------------------------------------------------------------- |
| snowplow_fractribution_report_table        | The main output table that shows conversions, revenue, spend and ROAS per channel.
| snowplow_fractribution_channel_attribution | The conversion and revenue attribution per channel (used to create the report table).
| snowplow_fractribution_path_summary_with_channels | For each unique path, a summary of conversions, non conversions and revenue, as well as which channels were assigned the contribution |

### Differences to Google's Fractribution

There are some changes from [Google's](https://github.com/google/fractribution) Fractribution code that have been noted below.

- Temporary UDFs have been converted to persistent / permanent UDFs
- Some temporary tables converted to permanent tables
- Users without a user_id are treated as 'anonymous' ('f') users and the domain_userid is used to identify these sessions
- Users with a user_id are treated as identified ('u') users
- Templating is now run almost entirely within dbt rather than the custom SQL / Jinja templating in the original Fractribution project
- Channel changes and contributions within a session can be considered using the `snowplow__consider_intrasession_channels` variable.

# Join the Snowplow community

We welcome all ideas, questions and contributions!

For support requests, please use our community support [Discourse][discourse] forum.

If you find a bug, please report an issue on GitHub.

# Copyright and license

The snowplow-fractribution package is Copyright 2022-2023 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license]: http://www.apache.org/licenses/LICENSE-2.0
[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat

[release-image]: https://img.shields.io/github/v/release/snowplow/dbt-snowplow-fractribution?sort=semver
[releases]: https://github.com/snowplow/dbt-snowplow-fractribution/releases

[tracker-classification]: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/tracker-maintenance-classification/
[early-release]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Early%20Release&color=014477&labelColor=9ba0aa&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC
[unsupported]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Unsupported&color=24292e&labelColor=lightgrey&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC
[maintained]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Maintained&color=9e62dd&labelColor=9ba0aa&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC
[actively-maintained]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Actively%20Maintained&color=6638b8&labelColor=9ba0aa&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC

[discourse-image]: https://img.shields.io/discourse/posts?server=https%3A%2F%2Fdiscourse.snowplow.io%2F
[discourse]: http://discourse.snowplow.io/
