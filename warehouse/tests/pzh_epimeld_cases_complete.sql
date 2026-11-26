-- pzh_epimeld_cases: every year from the first to the last has Poland rows, and in each bulletin's own year the
-- voivodeships of a disease add up to the POLSKA row of the same table (voivodeships without a case are not printed).
-- A table that stops being read, a voivodeship lost or read twice, or a value read from the wrong column fails here.
-- One test per Silver model.
with t as (select * from {{ ref('pzh_epimeld_cases') }}),

years as (
    select unnest(generate_series(min(period_year), max(period_year))) as period_year
    from t
),

missing_years as (
    select
        y.period_year,
        'no Poland rows' as problem,
        cast(null as varchar) as disease_codes,
        cast(null as bigint) as voivodeships,
        cast(null as bigint) as poland
    from years as y
    where not exists (
        select 1 from t
        where t.period_year = y.period_year and t.area_code = 'PL'
    )
),

sums as (
    select
        period_year,
        'voivodeships differ from Poland' as problem,
        disease_codes,
        sum(cases) as voivodeships,
        max(poland_cases_in_edition) as poland
    from t
    where area_code <> 'PL'
    group by period_year, disease_codes
    having sum(cases) is distinct from max(poland_cases_in_edition)
)

select * from missing_years
union all
select * from sums
