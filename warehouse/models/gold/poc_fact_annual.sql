{{ config(alias='fact_annual') }}
{# Gold PoC: one row per series × period, frequency A; date_key is the first day of the period.
   Keys: series_key → dim_series, measure_key → dim_measure, geography_key → dim_geography,
   date_key → dim_calendar, observation_status_key → dim_observation_status. #}
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
    raw_sha256
from ({{ poc_observations() }})
where frequency = 'A'
