-- ZUS yearbook (gold.build_zus_yearbook): every value lies in a plausible range for its measure and area, and no
-- voivodeship exceeds Poland. Payers: Poland about 2.1-2.8 M, the smallest voivodeship about 46,000; insured persons:
-- Poland about 14.5-16.3 M, the smallest voivodeship about 310,000 (editions 2009-2011 and 2021-2023, Dev trials on
-- test/zus-yearbook). The bounds catch a value read from the wrong column or with a lost digit group, not small
-- errors. A live test on the build stage, as no metrics model of these measures is in tests/metrics_value_range.sql.
-- Returns the rows outside.
with b as (
    select * from {{ ref('poc_build_zus_yearbook') }}
),

poland as (
    select
        measure_key,
        date_key,
        value
    from b
    where geography_key = 'PL'
)

select
    b.measure_key,
    b.geography_key,
    b.date_key,
    b.value,
    'outside the range' as problem
from b
where
    (b.measure_codes = 'payers' and (b.value < 20000 or b.value > 4000000))
    or (b.measure_codes = 'insured' and (b.value < 150000 or b.value > 25000000))
union all
select
    b.measure_key,
    b.geography_key,
    b.date_key,
    b.value,
    'above Poland' as problem
from b
inner join poland as p on b.measure_key = p.measure_key and b.date_key = p.date_key
where b.geography_key <> 'PL' and b.value >= p.value
