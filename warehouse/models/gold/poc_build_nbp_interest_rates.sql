{{ config(alias='build_nbp_interest_rates') }}
{# Gold build stage: official NBP interest rates (silver.nbp_policy_rates: reference, lombard, deposit,
   rediscount, discount) as monthly observations of Poland. Silver holds one row per rate and date of an MPC
   change; a rate holds until the next change, so Gold places the rate in force on the last day of each month
   (today for the current month), from the month of the rate's first value. One measure per rate. #}
with rates as (
    select
        rate_code,
        name_pl,
        name_en,
        effective_from,
        rate_pct,
        source_version,
        raw_sha256
    from {{ ref('nbp_policy_rates') }}
),

months as (
    select
        rate_code,
        cast(unnest(generate_series(
            date_trunc('month', min(effective_from)), date_trunc('month', current_date), interval 1 month
        )) as date) as date_key
    from rates
    group by rate_code
),

as_of as (  -- the day the month's value is read: its last day, today in the current month
    select
        rate_code,
        date_key,
        least(last_day(date_key), current_date) as as_of_date
    from months
)

select
    'nbp_interest_rates' as source_system,
    'policy_rates' as block_id,
    'NBP official interest rates' as block_name,
    'nbp_interest_rates:' || r.rate_code as measure_key,
    coalesce(r.name_en, r.rate_code) || ' (NBP), rate in force at the end of the month' as measure_name,
    r.rate_code as measure_codes,
    'pct' as unit_code,
    'percent per year' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'M' as frequency,
    a.date_key,
    strftime(a.date_key, '%Y-%m') as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(r.rate_pct as double) as value,
    'ok' as observation_status_key,
    r.source_version,
    r.raw_sha256
from as_of as a
asof inner join rates as r on a.rate_code = r.rate_code and a.as_of_date >= r.effective_from
