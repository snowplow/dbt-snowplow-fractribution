-- User supplied SQL script to extract total ad spend by channel.
--
-- Required output schema:
--  channel: STRING NOT NULL (Must match those in channel_definitions.sql.)
--  spend: FLOAT64 (Use the same monetary units as conversion revenue, and NULL if unknown.)
--
-- Note that all flags are passed into this template (e.g. conversion_window_start/end_date).


-- TODO: put in your own spend calculations per channel in the channel_spend macro in your own dbt project
-- the model assigns an example 10k spend to each channel found in channel_counts


{{ channel_spend() }}