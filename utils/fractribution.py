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

"""Library for computing fractional attribution."""

import io
import json
import re
from typing import Iterable, List, Mapping, Tuple



class _PathSummary(object):
    """Stores conversion and attribution information.

    To save space, the path itself is not stored here, as it is already stored
    as the key of the _path_tuple_to_summary dict in Fractribution.
    """

    def __init__(self, conversions: int, non_conversions: int, revenue: float):
        self.conversions = conversions
        self.non_conversions = non_conversions
        self.revenue = revenue
        self.channel_to_attribution = {}


class Fractribution(object):
    """Runs Fractribution on a set of marketing paths to (non-)conversion."""

    @classmethod
    def _get_path_string(cls, path_tuple: Iterable[str]) -> str:
        return " > ".join(path_tuple)

    def __init__(self, query_job):
        """Loads (path_str, conversions, non_conversions, revenue) from query_job.

        Args:
          query_job: QueryJob of (path_str, conversions, non_conversions, revenue).
        """
        self._path_tuple_to_summary = {}

        for (path_str, conversions, non_conversions, revenue) in query_job:
            path_tuple = ()
            if path_str:
                path_tuple = tuple(path_str.split(" > "))
            if path_tuple not in self._path_tuple_to_summary:
                self._path_tuple_to_summary[path_tuple] = _PathSummary(
                    conversions, non_conversions, revenue
                )
            else:
                path_summary = self._path_tuple_to_summary[path_tuple]
                path_summary.conversions += conversions
                path_summary.non_conversions += non_conversions

    def _get_conversion_probability(self, path_tuple: Tuple[str, ...]) -> float:
        """Returns path_tuple conversion/(conversion+non_conversion) probability.

        Args:
          path_tuple: Tuple of channel names in the path.

        Returns:
          Conversion probability of customers with this path.
        """

        if path_tuple not in self._path_tuple_to_summary:
            return 0.0
        path_summary = self._path_tuple_to_summary[path_tuple]
        count = path_summary.conversions + path_summary.non_conversions
        if not count:
            return 0.0
        return path_summary.conversions / count

    def _get_counterfactual_marginal_contributions(
        self, path_tuple: Tuple[str, ...]
    ) -> List[float]:
        """Returns the marginal contribution of each channel in the path.

        Args:
          path_tuple: Tuple of channel names in the path.

        Returns:
          List of marginal contribution values, one for each channel in path_tuple.
        """
        if not path_tuple:
            return []
        marginal_contributions = [0] * len(path_tuple)
        path_conversion_probability = self._get_conversion_probability(path_tuple)
        # If the path contains a single channel, it gets 100% of the contribution.
        if len(path_tuple) == 1:
            marginal_contributions[0] = path_conversion_probability
        else:
            # Otherwise, compute the counterfactual marginal contributions by channel.
            for i in range(len(path_tuple)):
                counterfactual_tuple = path_tuple[:i] + path_tuple[i + 1 :]
                raw_marginal_contribution = (
                    path_conversion_probability
                    - self._get_conversion_probability(counterfactual_tuple)
                )
                # Avoid negative contributions by flooring to 0.
                marginal_contributions[i] = max(raw_marginal_contribution, 0)
        return marginal_contributions

    def run_fractribution(self, attribution_model: str) -> None:
        """Runs Fractribution with the given attribution_model.

        Side-effect: Updates channel_to_attribution dicts in _path_tuple_to_summary.

        Args:
          attribution_model: Must be a key in ATTRIBUTION_MODELS
        """
        self.ATTRIBUTION_MODELS[attribution_model](self)

    def run_shapley_attribution(self) -> None:
        """Compute fractional attribution values for all given paths.

        Side-effect: Updates channel_to_attribution dicts in _path_tuple_to_summary.
        """
        print("running shapley attribution...")

        print("input items:", len(self._path_tuple_to_summary.items()))

        for path_tuple, path_summary in self._path_tuple_to_summary.items():
            # Ignore empty paths, which can happen when there is a conversion, but
            # no matching marketing channel events. Also ignore paths with no
            # conversions, since there is no attribution to make.
            if not path_tuple or not path_summary.conversions:
                continue
            path_summary.channel_to_attribution = {}
            marginal_contributions = self._get_counterfactual_marginal_contributions(
                path_tuple
            )
            sum_marginal_contributions = sum(marginal_contributions)
            if sum_marginal_contributions:
                marginal_contributions = [
                    marginal_contribution / sum_marginal_contributions
                    for marginal_contribution in marginal_contributions
                ]
            # Use last touch attribution if no channel has a marginal_contribution.
            if sum_marginal_contributions == 0:
                marginal_contributions[-1] = 1
            # Aggregate the marginal contributions by channel, as channels can occur
            # more than once in the path.
            for i, channel in enumerate(path_tuple):
                path_summary.channel_to_attribution[channel] = marginal_contributions[
                    i
                ] + path_summary.channel_to_attribution.get(channel, 0.0)

    def run_first_touch_attribution(self) -> None:
        """Assigns 100% attribution to the first channel in each path.

        Side-effect: Updates channel_to_attribution dicts in _path_tuple_to_summary.
        """
        print("running first_touch attribution...")
        for path_tuple, path_summary in self._path_tuple_to_summary.items():
            path_summary.channel_to_attribution = {}
            if not path_tuple:
                continue
            for channel in path_tuple:
                path_summary.channel_to_attribution[channel] = 0.0
            path_summary.channel_to_attribution[path_tuple[0]] = 1

    def run_last_touch_attribution(self) -> None:
        """Assigns 100% attribution to the last channel in each path.

        Side-effect: Updates channel_to_attribution dicts in _path_tuple_to_summary.
        """
        print("running last_touch attribution...")
        for path_tuple, path_summary in self._path_tuple_to_summary.items():
            path_summary.channel_to_attribution = {}
            if not path_tuple:
                continue
            for channel in path_tuple:
                path_summary.channel_to_attribution[channel] = 0.0
            path_summary.channel_to_attribution[path_tuple[-1]] = 1

    def run_linear_attribution(self) -> None:
        """Assigns attribution evenly between all channels on the path.

        Side-effect: Updates channel_to_attribution dicts in _path_tuple_to_summary.
        """
        print("running linear attribution...")
        for path_tuple, path_summary in self._path_tuple_to_summary.items():
            path_summary.channel_to_attribution = {}
            if not path_tuple:
                continue
            credit = 1.0 / len(path_tuple)
            for channel in path_tuple:
                path_summary.channel_to_attribution[channel] = (
                    path_summary.channel_to_attribution.get(channel, 0.0) + credit
                )

    def run_position_based_attribution(self) -> None:
        """Assigns attribution using the position based algorithm.

        The first and last channels get 40% of the credit each, with the remaining
        channels getting the leftover 20% distributed evenly.

        Side-effect: Updates channel_to_attribution dicts in _path_tuple_to_summary.
        """
        print("running position_based attribution...")
        for path_tuple, path_summary in self._path_tuple_to_summary.items():
            path_summary.channel_to_attribution = {}
            if not path_tuple:
                continue
            path_summary.channel_to_attribution[path_tuple[0]] = 0.4
            path_summary.channel_to_attribution[path_tuple[-1]] = (
                path_summary.channel_to_attribution.get(path_tuple[-1], 0) + 0.4
            )
            leftover_credit = 0
            middle_path = []
            if len(path_tuple) == 1:
                # All the leftover credit goes to the first and only channel
                leftover_credit = 0.2
                middle_path = path_tuple
            elif len(path_tuple) == 2:
                # The leftover credit is split between the two channels in the path.
                leftover_credit = 0.1
                middle_path = path_tuple
            else:
                # The leftover credit is evenly distributed among the middle channels.
                leftover_credit = 0.2 / (len(path_tuple) - 2)
                middle_path = path_tuple[1:-1]
            for channel in middle_path:
                path_summary.channel_to_attribution[channel] = (
                    path_summary.channel_to_attribution.get(channel, 0.0)
                    + leftover_credit
                )

    def normalize_channel_to_attribution_names(self) -> None:
        """Normalizes channel names and aggregates attribution values if necessary.

        Path transforms can also transform channel names to include a count
        related suffix (<COUNT>). This function undoes the transform on the channel
        name by removing the suffix, so that a single channel with two different
        suffixes can be aggregated.

        Side-effect: Updates channel_to_attribution names in _path_tuple_to_summary.
        """
        for path_summary in self._path_tuple_to_summary.values():
            channel_to_attribution = {}
            for channel in path_summary.channel_to_attribution:
                normalized_channel = re.sub(r"\(.*", "", channel)
                channel_to_attribution[normalized_channel] = (
                    channel_to_attribution.get(normalized_channel, 0)
                    + path_summary.channel_to_attribution[channel]
                )
            path_summary.channel_to_attribution = channel_to_attribution

    def _path_summary_to_json_stringio(self) -> io.BytesIO:
        """Returns a BytesIO file with one JSON-encoded _PathSummary per line."""

        bytesio = io.BytesIO()
        for path_tuple, path_summary in self._path_tuple_to_summary.items():
            row = {
                "transformed_path": self._get_path_string(path_tuple),
                "conversions": path_summary.conversions,
                "non_conversions": path_summary.non_conversions,
                "revenue": path_summary.revenue,
            }
            if path_summary.channel_to_attribution:
                row.update(path_summary.channel_to_attribution)
            bytesio.write(json.dumps(row).encode("utf-8"))
            bytesio.write("\n".encode("utf-8"))
        bytesio.flush()
        bytesio.seek(0)
        return bytesio

    def _path_summary_to_list(self) -> List:
        """Returns a list with list _PathSummary per line."""
        rows = []
        for path_tuple, path_summary in self._path_tuple_to_summary.items():
            row = {
                "transformed_path": self._get_path_string(path_tuple),
                "conversions": path_summary.conversions,
                "non_conversions": path_summary.non_conversions,
                "revenue": path_summary.revenue,
            }
            if path_summary.channel_to_attribution:
                row.update(path_summary.channel_to_attribution)
            rows.append(row)

        return rows

    def _get_channel_to_attribution(self) -> Mapping[str, float]:
        """Returns a mapping from channel to overall conversion attribution.

        Returns:
          Mapping from channel to overall conversion attribution.
        """
        overall_channel_to_attribution = {}
        for path_summary in self._path_tuple_to_summary.values():
            channel_to_attribution = path_summary.channel_to_attribution

            for channel, attribution in channel_to_attribution.items():
                overall_channel_to_attribution[channel] = (
                    overall_channel_to_attribution.get(channel, 0.0)
                    + attribution * path_summary.conversions
                )
        return overall_channel_to_attribution

    def _get_channel_to_revenue(self) -> Mapping[str, float]:
        """Returns a mapping from channel to overall revenue attribution.

        Returns:
          Mapping from channel to overall revenue attribution.
        """
        overall_channel_to_revenue = {}
        for path_summary in self._path_tuple_to_summary.values():
            channel_to_attribution = path_summary.channel_to_attribution
            revenue = path_summary.revenue
            if not revenue:
                revenue = 0.0
            for channel, attribution in channel_to_attribution.items():
                overall_channel_to_revenue[channel] = overall_channel_to_revenue.get(
                    channel, 0.0
                ) + attribution * float(revenue)
        return overall_channel_to_revenue

    ATTRIBUTION_MODELS = {
        "shapley": run_shapley_attribution,
        "first_touch": run_first_touch_attribution,
        "last_touch": run_last_touch_attribution,
        "position_based": run_position_based_attribution,
        "linear": run_linear_attribution,
    }
