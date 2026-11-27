{# PSE Reports API v2, report rce-pln (register PL-ENV-019): the market energy price RCE (rynkowa cena energii
   elektrycznej), PLN/MWh, one row per 15-minute period of each business day since 2024-06-14, from the newest copy of
   each day's file (bronze.pse_reports_rce_pln; entity rce_pln_<YYYYMMDD>, version = the day's newest
   publication_ts_utc). Grain: business_date x dtime_utc.
   Times: dtime_utc is the UTC end of the period; dtime, period and period_utc are the published texts (dtime is the
   local end, and on the autumn DST day PSE marks the repeated hour with an 'a': '2025-10-26 02a:15:00', period
   '03:00 - 02a:15', so dtime stays text). dtime_local is the local (Europe/Warsaw) end as a timestamp: dtime_utc plus
   utc_offset_hours, the offset in force at the start of the period (1 in winter, 2 in summer; taken at the start
   because the end of the last period before a switch is the switch instant itself); it equals dtime without the 'a',
   except at a switch instant, which PSE labels with either offset (2025-03-30 '02:00', 2026-03-29 '03:00'; checked
   by tests/pse_reports_rce_pln_complete.sql). publication_ts (local) stays text for the same reason, beside
   the typed publication_ts_utc. periods_expected is the number of 15-minute periods of the
   local day (96; 92 and 100 on the DST days), the completeness reference of that test and of the Gold daily means.
   Values are cast strictly: a text that is not a number fails the build instead of becoming null. RCE can be negative
   (hours of surplus generation), so no lower bound applies. A tie of two copies of a day at the same version stays as
   two copies and fails the unique test, never resolved silently. #}
with bronze as (
    select
        *,
        rank() over (partition by entity order by source_version desc, ingested_at desc) as version_rank
    from {{ source('pse_reports', 'pse_reports_rce_pln') }}
),

timed as (
    select
        *,
        cast(dtime_utc as timestamp) as end_utc,
        cast(dtime_utc as timestamp) - interval 15 minute as start_utc
    from bronze
    where version_rank = 1
),

offsets as (
    select
        *,
        cast(
            (epoch(cast(timezone('Europe/Warsaw', timezone('UTC', start_utc)) as timestamp)) - epoch(start_utc)) / 3600
            as integer
        ) as utc_offset_hours
    from timed
)

select
    entity,
    cast(business_date as date) as business_date,
    period,
    period_utc,
    dtime,
    end_utc + to_hours(utc_offset_hours) as dtime_local,
    end_utc as dtime_utc,
    utc_offset_hours,
    cast((
        epoch(timezone('Europe/Warsaw', cast(cast(business_date as date) + 1 as timestamp)))
        - epoch(timezone('Europe/Warsaw', cast(cast(business_date as date) as timestamp)))
    ) / 900 as integer) as periods_expected,
    cast(rce_pln as double) as rce_pln,
    publication_ts,
    cast(publication_ts_utc as timestamp) as publication_ts_utc,
    source_version,
    raw_sha256,
    ingested_at,
    source_row_number
from offsets
