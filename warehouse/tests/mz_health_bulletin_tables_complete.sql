-- mz_health_bulletin (models/silver/mz_health_bulletin): every reference year from 2018 (edition 2019, the first landed)
-- to the newest has each of the 35 modelled measures for all 17 areas (Poland and the 16 voivodeships) in its latest
-- edition, except what the source did not publish: licensed doctors and dentists for 2022 (the 2023 edition has no
-- such table). A title, header or area label that stops matching in a new edition fails here instead of thinning the
-- series. Refs only its own model.
with counts as (
    select
        table_kind || '.' || profession_code || '.' || measure_code as measure,
        period_year,
        count(distinct area_name) as n
    from {{ ref('mz_health_bulletin_tables') }}
    where is_latest_edition
    group by all
),

measures as (
    select unnest([
        {%- for k in ['licensed'] %}{% for p in ['doctors', 'dentists', 'nurses', 'midwives'] %}{% for m in ['count', 'per_10k'] %}
        '{{ k }}.{{ p }}.{{ m }}',
        {%- endfor %}{% endfor %}{% endfor %}
        {%- for p in ['doctors', 'dentists', 'pharmacists', 'lab_diagnosticians', 'nurses', 'midwives', 'physiotherapists', 'paramedics', 'dietitians'] %}{% for m in ['count', 'per_10k'] %}
        'employed.{{ p }}.{{ m }}',
        {%- endfor %}{% endfor %}
        'hospitals.hospitals.count', 'hospitals.beds.count', 'hospitals.beds.per_10k',
        'hospital_activity.inpatients.count', 'hospital_activity.inpatients.per_bed',
        'hospital_activity.patient_days.count', 'hospital_activity.length_of_stay.mean',
        'hospital_activity.bed_occupancy.days', 'hospital_activity.bed_occupancy.pct'
    ]) as measure
),

years as (
    select unnest(generate_series(2018, max(period_year))) as period_year
    from counts
),

grid as (
    select
        m.measure,
        y.period_year
    from measures as m
    cross join years as y
    where not (y.period_year = 2022 and regexp_matches(m.measure, '^licensed[.](doctors|dentists)[.]'))
)

select
    coalesce(g.measure, c.measure) as measure,
    coalesce(g.period_year, c.period_year) as period_year,
    coalesce(c.n, 0) as found,
    17 as expected
from grid as g
full outer join counts as c on g.measure = c.measure and g.period_year = c.period_year
where coalesce(c.n, 0) <> 17
