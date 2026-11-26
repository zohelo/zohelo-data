-- RSPO register entries (models/silver/rspo/rspo_institutions.sql): every school year from 2020/2021 to the newest
-- list is present with at least 50,000 entries in all 16 voivodeships (each list 2020-2025 holds 55,500 to 55,750),
-- and every gmina code lies in its powiat and every powiat in its voivodeship. Returns one row per failure.
with years as (
    select
        reference_year,
        count(*) as entries,
        count(distinct voivodeship_teryt) as voivodeships
    from {{ ref('rspo_institutions') }}
    group by reference_year
),

expected as (
    select unnest(range(2020, (select max(reference_year) from years) + 1)) as reference_year
)

select
    'year ' || e.reference_year as check_name,
    coalesce(y.entries, 0) as found
from expected as e
left join years as y on e.reference_year = y.reference_year
where coalesce(y.entries, 0) < 50000 or coalesce(y.voivodeships, 0) <> 16
union all
select
    'teryt ' || school_year || ' ' || rspo_number as check_name,
    1 as found
from {{ ref('rspo_institutions') }}
where
    not starts_with(powiat_teryt, voivodeship_teryt)
    or not starts_with(gmina_teryt, powiat_teryt)
    or length(powiat_teryt) <> 4
    or length(gmina_teryt) <> 7
