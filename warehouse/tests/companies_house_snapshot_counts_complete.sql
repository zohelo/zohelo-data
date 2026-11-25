-- Companies House (#1502): silver.companies_house_snapshot_counts is complete. The counts of the snapshot Bronze
-- holds add up to Bronze's rows (every company counted once); the grain snapshot_date × country_of_origin ×
-- company_category × company_status is unique, its keys other than the country (blank in the file) are not null;
-- and the build stage's on_register total of each month equals the counts of that month's newest snapshot.
-- Returns one row per failed check (found <> expected).
with counts as (
    select *
    from {{ source('companies_house_silver', 'companies_house_snapshot_counts') }}
),

bronze as (
    select
        cast(snapshot_date as date) as snapshot_date,
        count(*) as companies
    from {{ source('companies_house', 'companies_house_companies') }}
    group by all
),

per_snapshot as (
    select
        snapshot_date,
        reference_month,
        sum(companies) as companies
    from counts
    group by all
),

per_month as (
    select
        reference_month,
        companies
    from per_snapshot
    qualify snapshot_date = max(snapshot_date) over (partition by reference_month)
),

built as (
    select
        date_key,
        value
    from {{ ref('poc_build_companies_house') }}
    where measure_key = 'companies_house:basic_company_data:companies:on_register' and is_total
),

repeated as (
    select count(*) as n
    from (
        select
            snapshot_date,
            country_of_origin,
            company_category,
            company_status
        from counts
        group by all
        having count(*) > 1
    ) as r
)

select
    'bronze snapshot counted in silver' as check_name,
    cast(b.snapshot_date as varchar) as at_date,
    cast(coalesce(s.companies, 0) as double) as found,
    cast(b.companies as double) as expected
from bronze as b
left join per_snapshot as s on b.snapshot_date = s.snapshot_date
where coalesce(s.companies, 0) <> b.companies
union all
select
    'gold on_register total = newest snapshot of the month' as check_name,
    cast(m.reference_month as varchar) as at_date,
    coalesce(g.value, -1) as found,
    cast(m.companies as double) as expected
from per_month as m
left join built as g on m.reference_month = g.date_key
where g.value is null or g.value <> m.companies
union all
select
    'silver grain repeated' as check_name,
    null as at_date,
    cast(n as double) as found,
    0 as expected
from repeated
where n > 0
union all
select
    'silver null keys' as check_name,
    null as at_date,
    cast(count(*) as double) as found,
    0 as expected
from counts
where
    snapshot_date is null or reference_month is null or company_category is null or company_status is null
    or companies is null or raw_sha256 is null
having count(*) > 0
