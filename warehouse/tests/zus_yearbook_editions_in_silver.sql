-- zus_yearbook: every edition in Bronze yields Silver rows for both tables (payers and insured persons). A title or
-- layout the Silver model no longer finds in a new edition would otherwise leave that edition's three years out
-- silently; returns the edition and table without Silver rows.
with editions as (
    select distinct entity as edition
    from {{ source('zus_yearbook', 'zus_yearbook_lines') }}
),

found as (
    select distinct
        edition,
        measure_code
    from {{ ref('zus_yearbook_tables') }}
)

select
    b.edition,
    m.measure_code
from editions as b
cross join (values ('payers'), ('insured')) as m (measure_code)
left join found as s on b.edition = s.edition and m.measure_code = s.measure_code
where s.edition is null
