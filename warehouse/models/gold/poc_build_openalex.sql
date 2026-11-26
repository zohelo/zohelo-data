{{ config(alias='build_openalex', tags=['gold_block']) }}
{# Gold build stage: OpenAlex (register CD-028, #2996), annual counts, area Poland (source code 'PL'). One frequency
   (A) per measure_key.
   Block 'works_pl' (silver.openalex_works_pl_counts): works with an author affiliated with an institution in Poland
   by publication year: openalex:works_pl:works (all), :works:open_access, and :works:field {field: OpenAlex field
   id, label the field name} (fields do not overlap; works without a primary topic are in none, so the fields add up
   to less than all).
   Block 'institutions_pl' (silver.openalex_institution_works_by_year): per institution in Poland {institution:
   OpenAlex id, label its name} openalex:institutions_pl:works:institution and :works:open_access:institution by
   year (the recent years OpenAlex lists, 2010 onwards; not additive over institutions).
   The current year: kept as published at the read, status 'provisional' (the year of source_version, which is the
   read's time); past years are revised by OpenAlex too and are replaced by every read. Years after the read and
   before 1900 (the calendar's range) are not loaded. #}
with geo as (
    select geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'openalex' and source_code = 'PL'
),

works as (
    select
        'works_pl' as block_id,
        'openalex:works_pl:works' || case selection when 'all' then '' else ':' || selection end as measure_key,
        'Works with an author affiliated with an institution in Poland (OpenAlex)'
        || case selection
            when 'open_access' then ', open access'
            when 'field' then ', by OpenAlex field of the primary topic'
            else ''
        end || ', by publication year' as measure_name,
        selection as measure_codes,
        'works' as unit_code,
        publication_year as period_year,
        case when selection = 'field' then cast(to_json(map(['field'], [field_id])) as varchar) else '{}' end
            as breakdown_json,
        case when selection = 'field' then cast(to_json(map(['field'], [field_name])) as varchar) end
            as breakdown_label_json,
        selection <> 'field' as is_total,
        works as value,
        source_version,
        raw_sha256
    from {{ ref('openalex_works_pl_counts') }}
),

institutions as (
    select
        'institutions_pl' as block_id,
        'openalex:institutions_pl:works' || m.suffix || ':institution' as measure_key,
        'Works with an author affiliated with the institution' || m.label
        || ', by publication year (OpenAlex; institutions in Poland)' as measure_name,
        ltrim(m.suffix || ':institution', ':') as measure_codes,
        'works' as unit_code,
        y.year as period_year,
        cast(to_json(map(['institution'], [y.institution_id])) as varchar) as breakdown_json,
        cast(to_json(map(['institution'], [i.display_name])) as varchar) as breakdown_label_json,
        false as is_total,
        case m.suffix when '' then y.works_count else y.oa_works_count end as value,
        y.source_version,
        y.raw_sha256
    from {{ ref('openalex_institution_works_by_year') }} as y
    inner join {{ ref('openalex_institutions_pl') }} as i on y.institution_id = i.institution_id
    cross join (values ('', ''), (':open_access', ', open access')) as m (suffix, label)
),

observations as (
    select * from works
    union all
    select * from institutions
)

select
    'openalex' as source_system,
    o.block_id,
    case o.block_id
        when 'works_pl' then 'OpenAlex: works with an author affiliated with an institution in Poland, annual'
        else 'OpenAlex: works and open-access works per institution in Poland, annual'
    end as block_name,
    o.measure_key,
    o.measure_name,
    o.measure_codes,
    o.unit_code,
    'works' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.period_year, 1, 1) as date_key,
    cast(o.period_year as varchar) as period_label,
    o.breakdown_json,
    o.breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.is_total,
    cast(o.value as double) as value,
    case when o.period_year = cast(left(o.source_version, 4) as integer) then 'provisional' else 'ok' end
        as observation_status_key,
    o.source_version,
    o.raw_sha256
from observations as o
left join geo as g on true
where o.period_year between 1900 and cast(left(o.source_version, 4) as integer)
