-- gis_sanitary (register PLS-018): every edition between the first and the last is present, and each one is
-- complete: its years run without gaps up to its report year, every year has a total and a domestic row, each row
-- was pivoted from exactly two cells (samples and per cent), domestic samples do not exceed the total, and the
-- groups add up to the total to 1 / 1,000 of it (as in Bronze; 2020 is printed 50 short). Refs only this model.
with s as (
    select * from {{ ref('gis_sanitary_food_samples') }}
),

editions as (
    select unnest(generate_series(min(edition_year), max(edition_year))) as edition_year from s
),

per_edition as (
    select
        edition_year,
        min(reference_year) as first_year,
        max(reference_year) as last_year,
        count(distinct reference_year) as years,
        count(*) filter (where origin_code = 'total') as total_rows,
        count(*) filter (where origin_code = 'domestic') as domestic_rows,
        count(*) filter (where cell_count <> 2) as bad_rows,
        count(*) filter (where is_report_year and origin_code = 'total') as report_rows,
        count(*) filter (
            where origin_code = 'total' and abs(components_minus_total) > greatest(samples_examined // 1000, 0)
        ) as sum_rows
    from s
    group by edition_year
),

domestic as (
    select d.edition_year, count(*) as over_total
    from s as d
    inner join s as t
        on d.entity = t.entity and d.reference_year = t.reference_year and t.origin_code = 'total'
    where d.origin_code = 'domestic' and d.samples_examined > t.samples_examined
    group by d.edition_year
)

select
    e.edition_year,
    p.first_year,
    p.last_year,
    p.years,
    p.total_rows,
    p.domestic_rows,
    p.bad_rows,
    p.report_rows,
    p.sum_rows,
    d.over_total
from editions as e
left join per_edition as p on e.edition_year = p.edition_year
left join domestic as d on e.edition_year = d.edition_year
where not coalesce(
    p.last_year = e.edition_year
    and p.years = p.last_year - p.first_year + 1
    and p.total_rows = p.years
    and p.domestic_rows = p.years
    and p.bad_rows = 0
    and p.report_rows = 1
    and p.sum_rows = 0
    and d.over_total is null,
    false
)
