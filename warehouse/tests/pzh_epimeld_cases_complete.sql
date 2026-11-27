-- pzh_epimeld_cases: every year from the first to the last has Poland rows, and in each bulletin's own year the
-- voivodeships of a disease add up to the POLSKA row of the same block (voivodeships without a case are not printed;
-- blocks with a row Bronze could not read, or with a printed row whose quarters do not add up to its year (a misprint
-- in the bulletin, kept as published and flagged in Gold), are left out of the sum). A table that stops being read, a voivodeship
-- lost or read twice, or a value read from the wrong column fails here. One test per Silver model.
with t as (select * from {{ ref('pzh_epimeld_cases') }}),

years as (
    select unnest(generate_series(min(period_year), max(period_year))) as period_year
    from t
),

missing_years as (
    select
        y.period_year,
        'no Poland rows' as problem,
        cast(null as varchar) as disease_key,
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
        disease_key,
        sum(cases) as voivodeships,
        max(poland_cases_in_edition) as poland
    from t
    where area_code <> 'PL'
    group by period_year, disease_key
    having bool_and(table_read) and bool_and(block_quarters_match) and max(poland_cases_in_edition) is not null
        and sum(cases) <> max(poland_cases_in_edition)
)

select * from missing_years
union all
select * from sums
