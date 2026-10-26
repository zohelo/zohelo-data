{{ config(alias='build_bdl') }}
{# Gold PoC build stage: sampled GUS BDL subgroups as observations in the common shape.
   Coarse measure rule (to refine per domain in v1): measure = subgroup × measure unit × the
   first label (n1) when no variable of the subgroup has n1 = "ogółem" (then n1 names what is
   measured: "urodzenia żywe", "ludność na 1 km2"); the other labels are breakdowns; a series
   is total when every breakdown label is "ogółem".
   Sample: years from 2010, units down to powiat (gmina for Ludność from 2015 and for
   Powierzchnia / Gęstość zaludnienia). #}
with blocks as ({{ poc_bdl_blocks() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'gus_bdl'
),

units as (
    select
        unit_id,
        level
    from {{ ref('bdl_units') }}
),

variables as (select * from {{ ref('bdl_variables') }}),

n1_rule as (  -- n1 is measure-defining when the subgroup has no "ogółem" among its n1 labels
    select
        subgroup_id,
        bool_and(lower(n1) <> 'ogółem') as n1_defines_measure
    from variables
    where n1 is not null
    group by subgroup_id
),

silver as (
    select
        o.*,
        b.block_name,
        b.subject_path,
        coalesce(u.level, case when o.unit_code_scheme = 'TERC' then 6 end) as unit_level,
        v.n1,
        v.n2,
        v.n3,
        v.n4,
        v.n5,
        coalesce(r.n1_defines_measure, false) as n1_defines_measure,
        case when coalesce(r.n1_defines_measure, false) then cast(v.n1 as varchar) end as measure_label,
        list_filter(
            [
                case when coalesce(r.n1_defines_measure, false) then null else cast(v.n1 as varchar) end,
                cast(v.n2 as varchar), cast(v.n3 as varchar), cast(v.n4 as varchar), cast(v.n5 as varchar)
            ],
            x -> x is not null
        ) as labels
    from {{ source('silver_entities', 'bdl_observations') }} as o
    inner join blocks as b on o.subgroup_id = b.subgroup_id
    left join units as u on o.unit_code = u.unit_id
    left join variables as v on o.subgroup_id = v.subgroup_id and o.variable_id = v.variable_id
    left join n1_rule as r on o.subgroup_id = r.subgroup_id
    where o.value is not null and o.year >= 2010
),

kept as (
    select *
    from silver
    where
        case subgroup_id
            when 'P2137'
                then
                    (unit_level <= 5 or year >= 2015)
                    and len(list_filter(labels, x -> lower(x) not in ('ogółem', 'mężczyźni', 'kobiety'))) = 0
            when 'P1410' then true
            when 'P2425' then true
            else unit_level <= 5
        end
)

select
    'gus_bdl' as source_system,
    k.subgroup_id as block_id,
    k.block_name,
    'gus_bdl:'
    || k.subgroup_id
    || ':'
    || coalesce(k.measure_unit, '-')
    || coalesce(':' || k.measure_label, '') as measure_key,
    k.block_name || coalesce(' — ' || k.measure_label, '') || ' — ' || coalesce(k.measure_unit, '-') as measure_name,
    coalesce(k.measure_unit, '-') || coalesce(':' || k.measure_label, '') as measure_codes,
    coalesce(k.measure_unit, '-') as unit_code,
    coalesce(k.measure_unit, '-') as unit_name,
    k.unit_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(k.year, 1, 1) as date_key,
    cast(k.year as varchar) as period_label,
    cast(to_json(map(
        list_transform(generate_series(1, len(k.labels)), i -> 'n' || i),
        k.labels
    )) as varchar) as breakdown_json,
    cast(to_json(map(
        list_transform(generate_series(1, len(k.labels)), i -> 'n' || i),
        k.labels
    )) as varchar) as breakdown_label_json,
    case
        when list_contains(list_transform(k.labels, x -> lower(x)), 'mężczyźni') then 'M'
        when list_contains(list_transform(k.labels, x -> lower(x)), 'kobiety') then 'F'
        when list_contains(list_transform(k.labels, x -> lower(x)), 'ogółem') then 'T'
    end as sex_code,
    len(list_filter(k.labels, x -> lower(x) <> 'ogółem')) = 0 as is_total,
    k.value,
    case when k.value_conflict then 'conflicting_values' else 'ok' end as observation_status_key,
    k.source_version,
    k.raw_sha256
from kept as k
left join geo_map as g on k.unit_code = g.source_code
