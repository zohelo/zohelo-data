{# Gold PoC: one row per series × period, frequency M; date_key is the first day of the period.
   Keys: series_key → dim_series, measure_key → dim_measure, geography_key → dim_geography,
   date_key → dim_calendar, observation_status_key → dim_observation_status. #}
{{
    config(
        alias='fact_monthly',
        materialized='incremental',
        incremental_strategy='gold_blocks',
        on_schema_change='fail',
        full_refresh=poc_block_full_refresh('poc_fact_monthly'),
    )
}}
select
    {{ poc_series_key() }} as series_key,
    measure_key,
    geography_key,
    date_key,
    frequency,
    value,
    observation_status_key,
    source_system,
    block_id,
    period_label,
    source_version,
    raw_sha256,
    gold_block
from ({{ poc_observations(is_incremental()) }})
where frequency = 'M'
