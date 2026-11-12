{{ config(alias='dim_unit') }}
{# Gold: units of measure as the sources name them (unit_key = the source's own code, as published), with
   the conformed unit of warehouse/seeds/unit_map.csv (decision D33; it replaced a hand-written case).
   A code maps through the unit_map rows of its sources without a measure_key_pattern; a code whose
   meaning depends on the measure (WDI "as named", BDL "-") or that has no row stays as published:
   conformed_unit_key is null, conformed_unit repeats the source's name and `mapped` is false. #}
with used as (
    select
        source_system,
        unit_code,
        any_value(unit_name) as unit_name
    from ({{ poc_observations() }})
    where unit_code <> '_Z'  -- the special member is added below
    group by source_system, unit_code
),

mapped as (
    select
        u.unit_code,
        any_value(u.unit_name) as unit_name,
        list(distinct u.source_system order by u.source_system) as sources,
        list(distinct m.unit_key order by m.unit_key) filter (where m.unit_key is not null) as conformed_unit_keys
    from used as u
    left join {{ ref('unit_map') }} as m
        on
            u.source_system = m.source_system
            and u.unit_code = m.unit_code
            and coalesce(m.measure_key_pattern, '') = ''
    group by u.unit_code
)

select
    m.unit_code as unit_key,
    m.unit_name,
    case when len(m.conformed_unit_keys) = 1 then m.conformed_unit_keys[1] end as conformed_unit_key,
    case when len(m.conformed_unit_keys) = 1 then c.name_en else m.unit_name end as conformed_unit,
    coalesce(len(m.conformed_unit_keys) = 1, false) as mapped,
    m.sources
from mapped as m
left join {{ ref('unit') }} as c on m.conformed_unit_keys[1] = c.unit_key
union all
select
    '_Z' as unit_key,
    'Not applicable' as unit_name,
    '_Z' as conformed_unit_key,
    'not applicable' as conformed_unit,
    true as mapped,
    [] as sources
