{# PSE power-system values per 15-minute period (report his-wlk-cal of the PSE Reports API v2, register PL-ENV-019):
   one row per business day x period of the newest Landing copy of each report-day (bronze.pse_his_wlk_cal, entity
   his_wlk_cal_YYYYMMDD; version = newest publication time; PSE publishes a day after it ends and re-publishes it
   later). Grain: business_date x interval_end_utc. Values in MW as published, columns under PSE's codes: demand
   (power demand of the national system), wi (wind generation), pv (photovoltaic generation), jg, jga, jgm, jgm1,
   jgm2, jgo, jgw1, jgw2, jgz1, jgz2, jgz3 (generating-unit groups), jnwrb, swm_p, swm_np (cross-border exchange
   balances, parallel and non-parallel). Every column of the report is an actual value (no forecast columns in
   his-wlk-cal). JSON null, the API's only "no value" marker (whole columns such as jga, jgo, jgz1, jgz3, and the
   periods of a day not published yet), stays null, never 0. Local period ends of the repeated hour carry PSE's marker
   "02a"; the UTC end is the key. Business days from 2024-06-14. A non-numeric value fails the build. #}
{%- set values = ['demand', 'wi', 'pv', 'jg', 'jga', 'jgm', 'jgm1', 'jgm2', 'jgo', 'jgw1', 'jgw2', 'jgz1', 'jgz2', 'jgz3',
                  'jnwrb', 'swm_p', 'swm_np'] -%}
with newest as (
    select *
    from {{ source('pse', 'pse_his_wlk_cal') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    cast(business_date as date) as business_date,
    cast(dtime_utc as timestamp) as interval_end_utc,
    dtime as interval_end_local,
    period as period_local,
    period_utc,
    {%- for v in values %}
    cast({{ v }} as double) as {{ v }},
    {%- endfor %}
    cast(publication_ts_utc as timestamp) as published_at_utc,
    entity,
    source_version,
    raw_sha256
from newest
