{# PSE Reports API v2, report his-wlk-cal (register PL-ENV-019): the basic quantities of the national power system
   (KSE) after the day, MW (mean power of the 15-minute period), one row per period of each business day since
   2024-06-14, from the newest copy of each day's file (bronze.pse_reports_his_wlk_cal; entity his_wlk_cal_<YYYYMMDD>,
   version = the day's newest publication_ts_utc; PSE re-publishes corrected days, e.g. 2024-06-14 again on
   2026-01-18). Grain: business_date x dtime_utc.
   Value fields as published, codes kept: demand (power demand of the KSE), wi (wind generation), pv (photovoltaic
   generation) and the report's other quantities under PSE's codes (generating-unit groups jg, jga, jgm, jgm1, jgm2,
   jgo, jgw1, jgw2, jgz1, jgz2, jgz3, jnwrb; cross-border exchange balances swm_p, swm_np; signed). A field PSE does
   not publish is null, never 0 (Dev trial of #2870: jga, jgo, jgz1 and jgz3 null on every day so far; jgm2, jgw2 and
   jgz2 only in later periods); 0 is a published value (pv at night). Times as in pse_reports_rce_pln: dtime, period
   and period_utc as published text (the repeated autumn DST hour is marked '02a:15'), dtime_utc the UTC end,
   dtime_local the local end from dtime_utc and the offset at the start of the period, publication_ts as text beside
   the typed publication_ts_utc (the report is published at about 02:52 local, inside the repeated hour on the night of
   the autumn switch), periods_expected the periods of the local day. Values are cast strictly: a text that is not a
   number fails the build. #}
with bronze as (
    select
        *,
        rank() over (partition by entity order by source_version desc, ingested_at desc) as version_rank
    from {{ source('pse_reports', 'pse_reports_his_wlk_cal') }}
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
    cast(demand as double) as demand,
    cast(wi as double) as wi,
    cast(pv as double) as pv,
    cast(jg as double) as jg,
    cast(jga as double) as jga,
    cast(jgm as double) as jgm,
    cast(jgm1 as double) as jgm1,
    cast(jgm2 as double) as jgm2,
    cast(jgo as double) as jgo,
    cast(jgw1 as double) as jgw1,
    cast(jgw2 as double) as jgw2,
    cast(jgz1 as double) as jgz1,
    cast(jgz2 as double) as jgz2,
    cast(jgz3 as double) as jgz3,
    cast(jnwrb as double) as jnwrb,
    cast(swm_p as double) as swm_p,
    cast(swm_np as double) as swm_np,
    publication_ts,
    cast(publication_ts_utc as timestamp) as publication_ts_utc,
    source_version,
    raw_sha256,
    ingested_at,
    source_row_number
from offsets
