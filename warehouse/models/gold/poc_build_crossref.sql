{{ config(alias='build_crossref', tags=['gold_block']) }}
{# Gold build stage: Crossref works whose deposited author affiliation names Poland or Polska (register CD-029),
   annual counts of silver.crossref_works_pl_counts. Block 'works_pl', one frequency (A) per measure_key:
   crossref:works_pl:works (all works, every publication year 1900 .. the year of the copy, from the all-years
   request), and per year from 2000 the breakdowns crossref:works_pl:works:type {type}, :publisher {publisher}
   (the name Crossref groups by) and :funder {funder: registry id, label the name for Polish funders}. Breakdowns
   are not additive (a work has several funders; none for most) and a facet cut at 1,000 values lacks its tail.
   The year of the copy is still filling: status provisional. Years after it (scheduled issues) and before 1900
   (the calendar's range) are not loaded. Area: PL by the selection rule. #}
with geo as (
    select geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'crossref' and source_code = 'PL'
),

counts as (
    select
        *,
        cast(left(source_version, 4) as integer) as copy_year
    from {{ ref('crossref_works_pl_counts') }}
    where works is not null
),

observations as (
    select
        'crossref:works_pl:works' as measure_key,
        'Works with a Polish author affiliation (Crossref DOIs, by publication year)' as measure_name,
        'total' as measure_codes,
        publication_year,
        '{}' as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        true as is_total,
        works,
        copy_year,
        source_version,
        raw_sha256
    from counts
    where entity = 'works_pl_by_year' and facet = 'published'
    union all
    select
        'crossref:works_pl:works:' || m.code as measure_key,
        'Works with a Polish author affiliation by ' || m.code || ' (Crossref DOIs, by publication year)'
            as measure_name,
        m.code as measure_codes,
        c.publication_year,
        cast(to_json(map([m.code], [coalesce(c.funder_id, c.facet_value)])) as varchar) as breakdown_json,
        case when f.name is not null then cast(to_json(map([m.code], [f.name])) as varchar) end as breakdown_label_json,
        false as is_total,
        c.works,
        c.copy_year,
        c.source_version,
        c.raw_sha256
    from counts as c
    inner join (
        values ('type-name', 'type'), ('publisher-name', 'publisher'), ('funder-doi', 'funder')
    ) as m (facet, code) on c.facet = m.facet
    left join {{ ref('crossref_funders') }} as f on c.funder_id = f.funder_id
    where c.entity <> 'works_pl_by_year'
)

select
    'crossref' as source_system,
    'works_pl' as block_id,
    'Crossref works with a Polish author affiliation (annual)' as block_name,
    r.measure_key,
    r.measure_name,
    r.measure_codes,
    'works' as unit_code,
    'works (DOIs)' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(r.publication_year, 1, 1) as date_key,
    cast(r.publication_year as varchar) as period_label,
    r.breakdown_json,
    r.breakdown_label_json,
    cast(null as varchar) as sex_code,
    r.is_total,
    cast(r.works as double) as value,
    case when r.publication_year = r.copy_year then 'provisional' else 'ok' end as observation_status_key,
    r.source_version,
    r.raw_sha256
from observations as r
left join geo as g on true
where r.publication_year between 1900 and r.copy_year
