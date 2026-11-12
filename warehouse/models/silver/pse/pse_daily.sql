{# PSE daily values for Poland (register PL-ENV-019): the arithmetic mean of the published 15-minute values over each
   business day (Europe/Warsaw), for the RCE price (silver.pse_rce_pln, PLN/MWh: a simple mean of the period
   prices, not weighted by volume) and for demand, wind and photovoltaic generation (silver.pse_power_system, MW:
   the mean power over the day). Grain: business_date x measure_code. Only complete days: every period of the day
   (96; 92 on the last Sunday of March, 100 on the last Sunday of October) present with a value; a day with fewer
   periods or a null value (not published yet, or a gap) is left out, never averaged over what is there.
   interval_count keeps the number of periods averaged. Units as published, no rescaling; a share of demand is not
   derived here (a possible metrics model). #}
with intervals as (
    select
        business_date,
        'rce-pln' as report,
        'rce_pln' as measure_code,
        'pln_per_mwh' as unit_code,
        rce_pln as value,
        published_at_utc,
        source_version,
        raw_sha256
    from {{ ref('pse_rce_pln') }}
    {%- for code in ['demand', 'wi', 'pv'] %}
    union all
    select
        business_date,
        'his-wlk-cal' as report,
        '{{ code }}' as measure_code,
        'mw' as unit_code,
        {{ code }} as value,
        published_at_utc,
        source_version,
        raw_sha256
    from {{ ref('pse_power_system') }}
    {%- endfor %}
),

days as (
    select
        business_date,
        report,
        measure_code,
        unit_code,
        source_version,
        raw_sha256,
        count(*) as interval_rows,
        count(value) as interval_count,
        avg(value) as value,
        min(value) as value_min,
        max(value) as value_max,
        max(published_at_utc) as published_at_utc,
        case
            when month(business_date) = 3 and dayofweek(business_date) = 0 and day(business_date) >= 25 then 92
            when month(business_date) = 10 and dayofweek(business_date) = 0 and day(business_date) >= 25 then 100
            else 96
        end as expected_intervals
    from intervals
    group by business_date, report, measure_code, unit_code, source_version, raw_sha256
)

select
    business_date,
    report,
    measure_code,
    unit_code,
    value,
    value_min,
    value_max,
    interval_count,
    expected_intervals,
    published_at_utc,
    source_version,
    raw_sha256
from days
where interval_rows = expected_intervals and interval_count = expected_intervals
