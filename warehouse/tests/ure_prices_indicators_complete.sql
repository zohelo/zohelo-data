-- ure_prices: every page in Bronze yields Silver rows; the five series (indicator x frequency) are all there; each
-- series has every year or quarter from its first to its last period exactly once; and no series has an empty or
-- dashed cell between two published values (a value lost in the middle of a series, or a moved column). One test
-- per Silver model; returns one row per problem.
with series as (
    select
        indicator_code,
        frequency,
        period_year * 4 + coalesce(period_quarter, 1) - 1 as q,
        value_status
    from {{ ref('ure_prices_indicators') }}
),

bounds as (
    select
        indicator_code,
        frequency,
        min(q) as first_q,
        max(q) as last_q,
        min(q) filter (where value_status = 'ok') as first_ok,
        max(q) filter (where value_status = 'ok') as last_ok,
        count(*) as periods
    from series
    group by all
),

expected as (
    select *
    from (
        values
        ('electricity_competitive_market', 'A'), ('electricity_competitive_market', 'Q'),
        ('electricity_households', 'A'), ('gas_import_eu_efta', 'Q'), ('gas_import_non_eu_efta', 'Q')
    ) as t (indicator_code, frequency)
)

select
    'page without Silver rows: ' || b.entity as problem
from (select distinct entity from {{ source('ure_prices', 'ure_prices_cells') }}) as b
where b.entity not in (select distinct entity from {{ ref('ure_prices_indicators') }})
union all
select 'series missing: ' || e.indicator_code || ' ' || e.frequency as problem
from expected as e
left join bounds as b on e.indicator_code = b.indicator_code and e.frequency = b.frequency
where b.first_ok is null
union all
select 'gap or repeat: ' || indicator_code || ' ' || frequency as problem
from bounds
where periods <> case when frequency = 'A' then (last_q - first_q) // 4 + 1 else last_q - first_q + 1 end
union all
select 'no value inside the series: ' || s.indicator_code || ' ' || s.frequency || ' ' || s.q as problem
from series as s
inner join bounds as b on s.indicator_code = b.indicator_code and s.frequency = b.frequency
where s.value_status <> 'ok' and s.q > b.first_ok and s.q < b.last_ok
