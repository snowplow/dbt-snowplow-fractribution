
-- TODO: put in your own spend calculations per channel in the channel_spend macro in your own dbt project
-- By default, the model assigns an example 10k spend to each channel found in channel_counts


{{ channel_spend() }}