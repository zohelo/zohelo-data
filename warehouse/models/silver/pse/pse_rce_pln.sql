{# PSE market price of electricity RCE (rynkowa cena energii elektrycznej, report rce-pln of the PSE Reports API v2,
   register PL-ENV-019): one row per business day x 15-minute period of the newest Landing copy of each report-day
   (bronze.pse_rce_pln, entity rce_pln_YYYYMMDD; version = newest publication time). Grain: business_date x
   interval_end_utc. PLN/MWh as published; PSE publishes the prices of day D on day D-1 (publication_ts). Business
   days from 2024-06-14 (start of API v2); 96 periods a day, 92 on the last Sunday of March, 100 on the last Sunday
   of October. Local period ends of the repeated hour carry PSE's marker "02a" (e.g. "2025-10-26 02a:15:00"), so the
   local end is kept as text and the UTC end is the key. A non-numeric price fails the build. #}
with newest as (
    select *
    from {{ source('pse', 'pse_rce_pln') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    cast(business_date as date) as business_date,
    cast(dtime_utc as timestamp) as interval_end_utc,
    dtime as interval_end_local,
    period as period_local,
    period_utc,
    cast(rce_pln as double) as rce_pln,
    cast(publication_ts_utc as timestamp) as published_at_utc,
    entity,
    source_version,
    raw_sha256
from newest
