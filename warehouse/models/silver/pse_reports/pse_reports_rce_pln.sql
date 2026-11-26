{# PSE Reports API v2, report rce-pln (register PL-ENV-019): the market energy price RCE (rynkowa cena energii
   elektrycznej), PLN/MWh, one row per 15-minute period of each business day since 2024-06-14, from the newest copy of
   each day's file (bronze.pse_reports_rce_pln; entity rce_pln_<YYYYMMDD>, version = the day's newest
   publication_ts_utc). Grain: business_date x dtime_utc.
   Times as published: dtime is the local (Europe/Warsaw) end of the period, dtime_utc its UTC end, period and
   period_utc the interval text ('00:00 - 00:15'); utc_offset_hours is dtime - dtime_utc (1 in winter, 2 in summer).
   periods_expected is the number of 15-minute periods of the local day (96; 92 and 100 on the DST days), the
   completeness reference of tests/pse_reports_rce_pln_complete.sql and of the Gold daily means. Values are cast
   strictly: a text that is not a number fails the build instead of becoming null. RCE can be negative (hours of
   surplus generation), so no lower bound applies. A tie of two copies of a day at the same version stays as two
   copies and fails the unique test, never resolved silently. #}
with bronze as (
    select
        *,
        rank() over (partition by entity order by source_version desc, ingested_at desc) as version_rank
    from {{ source('pse_reports', 'pse_reports_rce_pln') }}
)

select
    entity,
    cast(business_date as date) as business_date,
    period,
    period_utc,
    cast(dtime as timestamp) as dtime,
    cast(dtime_utc as timestamp) as dtime_utc,
    cast(datediff('hour', cast(dtime_utc as timestamp), cast(dtime as timestamp)) as integer) as utc_offset_hours,
    cast((
        epoch(timezone('Europe/Warsaw', cast(cast(business_date as date) + 1 as timestamp)))
        - epoch(timezone('Europe/Warsaw', cast(cast(business_date as date) as timestamp)))
    ) / 900 as integer) as periods_expected,
    cast(rce_pln as double) as rce_pln,
    cast(publication_ts as timestamp) as publication_ts,
    cast(publication_ts_utc as timestamp) as publication_ts_utc,
    source_version,
    raw_sha256,
    ingested_at,
    source_row_number
from bronze
where version_rank = 1
