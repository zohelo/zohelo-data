-- Companies House (#1502): silver.companies_house_poland_origin is complete. Bronze holds the newest snapshot
-- only and the loader writes Silver from that file, so the Silver rows equal the Bronze rows whose country of
-- origin is POLAND, all from the Bronze file (raw_sha256), one row per company number, no null key; the Gold
-- entity table has one row per Silver row. Returns one row per failed check (found <> expected).
with bronze as (
    select
        raw_sha256,
        trim(country_of_origin) = 'POLAND' as is_poland
    from {{ source('companies_house', 'companies_house_companies') }}
),

silver as (
    select
        company_number,
        raw_sha256
    from {{ source('companies_house_silver', 'companies_house_poland_origin') }}
),

checks as (
    select
        'silver rows = bronze rows of origin POLAND' as check_name,
        (select count(*) from silver) as found,
        (select count(*) from bronze where is_poland) as expected
    union all
    select
        'silver rows not from the bronze file' as check_name,
        (select count(*) from silver where raw_sha256 not in (select distinct b.raw_sha256 from bronze as b))
            as found,
        0 as expected
    union all
    select
        'silver company numbers repeated or null' as check_name,
        (
            select count(*) - count(distinct company_number) + count(*) filter (where company_number is null)
            from silver
        ) as found,
        0 as expected
    union all
    select
        'gold rows = silver rows' as check_name,
        (select count(*) from {{ ref('company_uk_poland_origin') }}) as found,
        (select count(*) from silver) as expected
)

select
    check_name,
    found,
    expected
from checks
where found <> expected
