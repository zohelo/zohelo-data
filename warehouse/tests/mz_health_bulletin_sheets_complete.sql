-- mz_health_bulletin (models/silver/mz_health_bulletin): every edition from 2019 (the first XLSX edition) to the newest
-- landed has sheets, and each has at least one modelled sheet of each of the four table kinds. A retitled table (the
-- title no longer matches) leaves its kind without a sheet and fails here; a changed layout fails the status test of
-- the sheets model. Refs only its own model. Returns the edition x table kind without a modelled sheet.
with editions as (
    select unnest(generate_series(2019, max(edition_year))) as edition_year
    from {{ ref('mz_health_bulletin_sheets') }}
),

kinds as (
    select unnest(['licensed', 'employed', 'hospitals', 'hospital_activity']) as table_kind
),

modelled as (
    select
        edition_year,
        table_kind,
        count(*) as n
    from {{ ref('mz_health_bulletin_sheets') }}
    where status = 'modelled'
    group by all
)

select
    e.edition_year,
    k.table_kind,
    coalesce(m.n, 0) as modelled_sheets
from editions as e
cross join kinds as k
left join modelled as m on e.edition_year = m.edition_year and k.table_kind = m.table_kind
where m.n is null
