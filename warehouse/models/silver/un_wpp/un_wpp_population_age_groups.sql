{# UN World Population Prospects 2024, population on 1 July by broad age group, medium variant, from the
   five-year age groups of entity population_by_age5_medium (bronze.un_wpp_rows): one row per location × year,
   thousands as published (sums of the published groups). The 1.76 million five-year rows stay in Bronze (above
   the full-rebuild policy of ~1M rows, #822); these sums are what the dependency ratios need. Dependency ratios
   per 100 persons aged 15-64: old_age (65+), child (0-14), total. Years from 2024 are projections. #}
with file_rows as (
    select *
    from {{ source('un_wpp', 'un_wpp_rows') }}
    where entity = 'population_by_age5_medium'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

headers as (  -- one header row per file (source_row_number 1)
    select
        raw_sha256,
        string_split(field_names, ',') as f
    from file_rows
    where source_row_number = 1
),

newest as (
    select
        h.f,
        string_split(r.field_values, '|') as v,
        r.raw_sha256,
        r.source_version,
        r.ingested_at
    from file_rows as r
    inner join headers as h on r.raw_sha256 = h.raw_sha256
    where r.source_row_number > 1
),

rows_typed as (
    select
        cast(v[list_position(f, 'LocID')] as integer) as loc_id,
        v[list_position(f, 'Location')] as location,
        nullif(v[list_position(f, 'ISO2_code')], '') as iso2,
        nullif(v[list_position(f, 'ISO3_code')], '') as iso3,
        nullif(v[list_position(f, 'LocTypeName')], '') as loc_type,
        cast(v[list_position(f, 'Time')] as integer) as year,
        cast(v[list_position(f, 'AgeGrpStart')] as integer) as age_start,
        try_cast(nullif(v[list_position(f, 'PopTotal')], '') as double) as pop_total,
        try_cast(nullif(v[list_position(f, 'PopMale')], '') as double) as pop_male,
        try_cast(nullif(v[list_position(f, 'PopFemale')], '') as double) as pop_female,
        raw_sha256,
        source_version,
        ingested_at
    from newest
),

grouped as (
    select
        loc_id,
        any_value(location) as location,
        any_value(iso2) as iso2,
        any_value(iso3) as iso3,
        any_value(loc_type) as loc_type,
        year,
        count(*) as age_groups,
        sum(pop_total) as pop_total,
        sum(pop_male) as pop_male,
        sum(pop_female) as pop_female,
        sum(pop_total) filter (where age_start < 15) as pop_0_14,
        sum(pop_total) filter (where age_start between 15 and 60) as pop_15_64,
        sum(pop_total) filter (where age_start >= 65) as pop_65_plus,
        any_value(raw_sha256) as raw_sha256,
        any_value(source_version) as source_version,
        any_value(ingested_at) as ingested_at
    from rows_typed
    group by loc_id, year
)

select
    loc_id,
    location,
    iso2,
    iso3,
    loc_type,
    year,
    year >= 2024 as is_projection,
    age_groups,
    pop_total,
    pop_male,
    pop_female,
    pop_0_14,
    pop_15_64,
    pop_65_plus,
    100 * pop_65_plus / nullif(pop_15_64, 0) as old_age_dependency_ratio,
    100 * pop_0_14 / nullif(pop_15_64, 0) as child_dependency_ratio,
    100 * (pop_0_14 + pop_65_plus) / nullif(pop_15_64, 0) as total_dependency_ratio,
    raw_sha256,
    source_version,
    ingested_at
from grouped
