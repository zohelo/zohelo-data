{# WGI observations of the newest bulk archive, typed: one row per economy × indicator × year.
   Six governance dimensions (VA voice and accountability, PV political stability, GE government
   effectiveness, RQ regulatory quality, RL rule of law, CC control of corruption), each as estimate
   (EST, about -2.5 to 2.5), score (SC, 0-100) with its 90 % bounds (SC_LB, SC_UB), standard error
   (SE) and number of sources (SR). Countries and territories only: WGI publishes no aggregates. #}
with newest as (
    select * from {{ source('world_bank_wgi', 'wgi_observations') }}
    where
        source_version
        = (select max(latest.source_version) from {{ source('world_bank_wgi', 'wgi_observations') }} as latest)
)

select
    trim(country_code) as economy_code,
    country_name as economy_name,
    trim(series_code) as indicator_code,
    cast(year as integer) as year,
    try_cast(value_text as double) as value,
    value_text,
    raw_sha256,
    source_version
from newest
