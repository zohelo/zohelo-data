-- PKW election results (models/silver/pkw/pkw_results.sql): every declared file must be in Silver with its areas
-- (16 voivodeships; 380 powiats; at least 2,400 gminas, the files differ in how cities with powiat rights and Warsaw's
-- districts are listed; Poland once in the sejmiki town/country file), and every value cell must read as a number
-- (a symbol or text in a value column fails here instead of becoming null or 0). One test per Silver model.
with expected (election_key, area_level, file_kind, min_areas, max_areas) as (
    values
    ('sejm2023', 'wojewodztwo', 'count', 16, 16),
    ('sejm2023', 'wojewodztwo', 'pct', 16, 16),
    ('sejm2023', 'powiat', 'count', 380, 380),
    ('sejm2023', 'powiat', 'pct', 380, 380),
    ('sejm2023', 'gmina', 'count', 2400, 2600),
    ('sejm2023', 'gmina', 'pct', 2400, 2600),
    ('pe2024', 'wojewodztwo', 'count', 16, 16),
    ('pe2024', 'wojewodztwo', 'pct', 16, 16),
    ('pe2024', 'powiat', 'count', 380, 380),
    ('pe2024', 'powiat', 'pct', 380, 380),
    ('pe2024', 'gmina', 'count', 2400, 2600),
    ('pe2024', 'gmina', 'pct', 2400, 2600),
    ('sejmiki2024', 'kraj', 'count', 1, 1),
    ('sejmiki2024', 'wojewodztwo', 'count', 16, 16),
    ('sejmiki2024', 'wojewodztwo', 'pct', 16, 16),
    ('sejmiki2024', 'powiat', 'count', 380, 380),
    ('sejmiki2024', 'powiat', 'pct', 380, 380),
    ('sejmiki2024', 'gmina', 'count', 2400, 2600),
    ('sejmiki2024', 'gmina', 'pct', 2400, 2600),
    ('prezydent2025_r1', 'wojewodztwo', 'count', 16, 16),
    ('prezydent2025_r1', 'powiat', 'count', 380, 380),
    ('prezydent2025_r1', 'gmina', 'count', 2400, 2600),
    ('prezydent2025_r2', 'wojewodztwo', 'count', 16, 16),
    ('prezydent2025_r2', 'powiat', 'count', 380, 380),
    ('prezydent2025_r2', 'gmina', 'count', 2400, 2600)
),

counts as (
    select
        election_key,
        area_level,
        file_kind,
        count(distinct area_key) filter (where area_kind not in ('abroad', 'ships')) as n
    from {{ ref('pkw_results') }}
    group by all
)

select
    coalesce(e.election_key, c.election_key) as election_key,
    coalesce(e.area_level, c.area_level) as area_level,
    coalesce(e.file_kind, c.file_kind) as file_kind,
    'areas' as problem,
    coalesce(c.n, 0) as found
from expected as e
full outer join counts as c
    on e.election_key = c.election_key and e.area_level = c.area_level and e.file_kind = c.file_kind
where e.election_key is null or c.election_key is null or c.n not between e.min_areas and e.max_areas

union all

select
    election_key,
    area_level,
    file_kind,
    'not a number: ' || column_label as problem,
    count(*) as found
from {{ ref('pkw_results') }}
where value is null
group by all
