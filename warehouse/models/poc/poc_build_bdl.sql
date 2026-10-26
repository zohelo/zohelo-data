{{ config(alias='build_bdl') }}
{# Gold PoC build stage: sampled GUS BDL subgroups as observations in the common shape.
   Coarse measure rule (to refine per domain in v1): measure = subgroup × measure unit × the
   label positions where no variable of the subgroup says "ogółem" (those name what is
   measured: "urodzenia żywe", "ludność na 1 km2", "mieszkania"); the other positions are
   breakdowns; a series is total when every breakdown label is "ogółem".
   Sample: years from 2010, units down to powiat (gmina for Ludność and Powierzchnia from 2015). #}
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

label_rule as (  -- a label position names what is measured when no variable of the subgroup has "ogółem" there
    select
        subgroup_id,
        bool_and(lower(cast(n1 as varchar)) <> 'ogółem') filter (where n1 is not null) as m1,
        bool_and(lower(cast(n2 as varchar)) <> 'ogółem') filter (where n2 is not null) as m2,
        bool_and(lower(cast(n3 as varchar)) <> 'ogółem') filter (where n3 is not null) as m3,
        bool_and(lower(cast(n4 as varchar)) <> 'ogółem') filter (where n4 is not null) as m4,
        bool_and(lower(cast(n5 as varchar)) <> 'ogółem') filter (where n5 is not null) as m5
    from variables
    group by subgroup_id
),

silver as (
    select
        o.*,
        b.block_name,
        b.subject_path,
        coalesce(
            u.level,
            case  -- a 7-digit web-export code: 0000000 Polska, WW00000 województwo, WWPP000 powiat, else gmina
                when o.unit_code = '0000000' then 0
                when regexp_matches(o.unit_code, '^[0-9]{2}00000$') then 2
                when regexp_matches(o.unit_code, '^[0-9]{4}000$') then 5
                else 6
            end
        ) as unit_level,
        v.n1,
        v.n2,
        v.n3,
        v.n4,
        v.n5,
        array_to_string(
            list_filter(
                [
                    case when coalesce(r.m1, false) then cast(v.n1 as varchar) end,
                    case when coalesce(r.m2, false) then cast(v.n2 as varchar) end,
                    case when coalesce(r.m3, false) then cast(v.n3 as varchar) end,
                    case when coalesce(r.m4, false) then cast(v.n4 as varchar) end,
                    case when coalesce(r.m5, false) then cast(v.n5 as varchar) end
                ],
                x -> x is not null
            ),
            ' / '
        ) as measure_label,
        list_filter(
            [
                case when coalesce(r.m1, false) then null else cast(v.n1 as varchar) end,
                case when coalesce(r.m2, false) then null else cast(v.n2 as varchar) end,
                case when coalesce(r.m3, false) then null else cast(v.n3 as varchar) end,
                case when coalesce(r.m4, false) then null else cast(v.n4 as varchar) end,
                case when coalesce(r.m5, false) then null else cast(v.n5 as varchar) end
            ],
            x -> x is not null
        ) as labels
    from {{ source('silver_entities', 'bdl_observations') }} as o
    inner join blocks as b on o.subgroup_id = b.subgroup_id
    left join units as u on o.unit_code = u.unit_id
    left join variables as v on o.subgroup_id = v.subgroup_id and o.variable_id = v.variable_id
    left join label_rule as r on o.subgroup_id = r.subgroup_id
    where o.value is not null and o.year >= 2010
),

kept as (
    select *
    from silver
    where
        -- a BDL "region" (level 3) that is a whole województwo duplicates the level-2 unit; only
        -- Mazowieckie's two NUTS 2 regions are areas of their own
        not (
            unit_level = 3
            and upper(unit_name) not like '%WARSZAWSKI STO%'
            and upper(unit_name) not like '%MAZOWIECKI REGIONALNY%'
        )
        -- the "makroregion województwo mazowieckie" (level 1) is the voivodeship itself (level 2)
        and not (unit_level = 1 and upper(unit_name) like '%MAZOWIECKIE%')
        and case subgroup_id
            when 'P2137'
                then
                    (unit_level <= 5 or year >= 2015)
                    and len(list_filter(labels, x -> lower(x) not in ('ogółem', 'mężczyźni', 'kobiety'))) = 0
            when 'P1410' then year >= 2015
            else unit_level <= 5
        end
),

mapped as (
    select
        k.*,
        coalesce(g.geography_key, '_X') as geography_key
    from kept as k
    left join geo_map as g on k.unit_code = g.source_code
),

deduplicated as (  -- one row per series × area × year: a subgroup can hold the same series under its
    -- website code and its API id (Silver finding #868); the API row wins, then the newest version
    select *
    from mapped
    qualify row_number() over (
        partition by subgroup_id, measure_unit, measure_label, labels, geography_key, year
        order by case when channel = 'api' then 0 else 1 end, source_version desc, unit_code asc
    ) = 1
)

select
    'gus_bdl' as source_system,
    k.subgroup_id as block_id,
    k.block_name,
    'gus_bdl:'
    || k.subgroup_id
    || ':'
    || coalesce(k.measure_unit, '-')
    || coalesce(':' || nullif(k.measure_label, ''), '') as measure_key,
    k.block_name
    || coalesce(' — ' || nullif(k.measure_label, ''), '')
    || ' — '
    || coalesce(k.measure_unit, '-') as measure_name,
    coalesce(k.measure_unit, '-') || coalesce(':' || nullif(k.measure_label, ''), '') as measure_codes,
    coalesce(k.measure_unit, '-') as unit_code,
    coalesce(k.measure_unit, '-') as unit_name,
    k.unit_code as geography_source_code,
    k.geography_key,
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
from deduplicated as k
