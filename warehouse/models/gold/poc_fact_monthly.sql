{# Gold PoC: one row per series × period, frequency M; date_key is the first day of the period.
   Keys: series_key → dim_series, measure_key → dim_measure, geography_key → dim_geography,
   date_key → dim_calendar, observation_status_key → dim_observation_status.
   Every column is cast to its type (poc_incremental.sql): an incremental run reads only the changed blocks. #}
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
    cast({{ poc_series_key() }} as varchar) as series_key,
    cast(measure_key as varchar) as measure_key,
    cast(geography_key as varchar) as geography_key,
    cast(date_key as date) as date_key,
    cast(frequency as varchar) as frequency,
    cast(value as double) as value,
    cast(observation_status_key as varchar) as observation_status_key,
    cast(source_system as varchar) as source_system,
    cast(block_id as varchar) as block_id,
    cast(period_label as varchar) as period_label,
    cast(source_version as varchar) as source_version,
    cast(raw_sha256 as varchar) as raw_sha256,
    cast(gold_block as varchar) as gold_block
from ({{ poc_observations(is_incremental()) }})
where frequency = 'M'
