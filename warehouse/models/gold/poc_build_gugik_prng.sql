{{ config(alias='build_gugik_prng', tags=['gold_block']) }}
{# Gold build stage (#2971): GUGiK PRNG, state register of geographical names (register PL-ENV-002), an entity snapshot
   of the newest package of each part (silver.prng_objects, latest-only: one date, the export date the package names).
   Counts of named objects located wholly or partly in each area: Poland (every object), every voivodeship, powiat and
   gmina whose TERYT code the register gives (silver.prng_object_gminas). An object spanning several units counts once
   in each, so the gminas of a powiat need not add up to it. Block miejscowosci: measure
   gugik_prng:miejscowosci:named_objects:count, breakdown object_kind (rodzaj obiektu: wieś, część wsi, miasto, ...).
   Block obiekty_fizjograficzne: measure gugik_prng:obiekty_fizjograficzne:named_objects:count, breakdown
   object_category (kategoria obiektu: hydrografia, ukształtowanie terenu, ...). '_T' is every kind or category
   (is_total). A code not in TERYT today maps to '_X' with the code kept. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gugik_prng'
),

objects as (
    select
        entity,
        prng_id,
        case entity when 'miejscowosci' then object_kind else object_category end as code,
        export_date
    from {{ ref('prng_objects') }}
),

-- one row per entity: a second package version in Silver repeats every count row and fails the grain test
provenance as (
    select
        entity,
        export_date,
        source_version,
        raw_sha256
    from {{ ref('prng_objects') }}
    group by all
),

membership as (
    select
        entity,
        prng_id,
        'PL' as area_code
    from objects
    union all
    select
        entity,
        prng_id,
        gmina_code
    from {{ ref('prng_object_gminas') }}
    union all
    select
        entity,
        prng_id,
        powiat_code
    from {{ ref('prng_object_gminas') }}
    union all
    select
        entity,
        prng_id,
        voivodeship_code
    from {{ ref('prng_object_gminas') }}
),

members as (
    select
        m.entity,
        m.area_code,
        m.prng_id,
        o.code
    from membership as m
    inner join objects as o on m.entity = o.entity and m.prng_id = o.prng_id
),

counts as (
    select
        entity,
        area_code,
        code,
        count(distinct prng_id) as n
    from members
    group by entity, area_code, code
    union all
    select
        entity,
        area_code,
        '_T' as code,
        count(distinct prng_id) as n
    from members
    group by entity, area_code
)

select
    'gugik_prng' as source_system,
    c.entity as block_id,
    case c.entity
        when 'miejscowosci' then 'GUGiK PRNG localities (state register of geographical names)'
        else 'GUGiK PRNG physiographic objects (state register of geographical names)'
    end as block_name,
    'gugik_prng:' || c.entity || ':named_objects:count' as measure_key,
    case c.entity
        when 'miejscowosci' then 'Localities in the state register of geographical names (PRNG)'
        else 'Physiographic objects in the state register of geographical names (PRNG)'
    end as measure_name,
    'count' as measure_codes,
    'named_objects' as unit_code,
    'named objects' as unit_name,
    c.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    p.export_date as date_key,
    cast(p.export_date as varchar) as period_label,
    cast(json_object(
        case c.entity when 'miejscowosci' then 'object_kind' else 'object_category' end, c.code
    ) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.code = '_T' as is_total,
    cast(c.n as double) as value,
    'ok' as observation_status_key,
    p.source_version,
    p.raw_sha256
from counts as c
inner join provenance as p on c.entity = p.entity
left join geo as g on c.area_code = g.source_code
