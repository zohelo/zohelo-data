{# The Silver tables read only for a source's own codes are optional (poc_optional_ref): a source before its
   first Bronze load adds no rows here and does not hold back the map and the blocks that read it. #}
{{ config(
    alias='map_geography',
    meta={'optional_parents': [
        'unctad_fdi_flows_stock', 'unctad_merchandise_trade', 'unctad_services_trade',
        'who_observations', 'unesco_uis_observations', 'faostat_observations',
    ]}
) }}
{# Gold PoC: every source's geography code → the area it means in dim_geography
   (design draft 2: map_geography__<source>, merged into one table). Unmatched codes map to
   '_X' and are counted by the build check. A source does not add a CTE here: it defines
   poc_geography__<source_system>() in warehouse/macros/sources/<source_system>.sql
   (docs/source-registration.md) and the two poc_geography_* calls below place it. The four sources still
   written here read their own Silver tables through poc_optional_ref, which repo_check pairs with this
   model's config meta `optional_parents`. #}
with geo as (select * from {{ ref('poc_dim_geography') }}),

{{ poc_geography_ctes() }}
-- UNCTAD: economies by UNCTAD code (M49; France 250 in the FDI, merchandise and services tables, 251 only in port
-- calls) from poc_unctad_areas: the EU27 members, 3412 European Union (2020 …) = EU27_2020, 0000 World. Other
-- economies have no ISO code in the tables (_X); the build stage builds only the listed areas, and
-- tests/unctad_areas_present.sql fails when a listed code is missing from one of the three built tables.
unctad_areas as ({{ poc_unctad_areas() }}),

unctad as (
    select
        'unctad' as source_system,
        c.economy as source_code,
        coalesce(g.geography_key, '_X') as geography_key
    from (
        select distinct economy from {{ poc_optional_ref('unctad_fdi_flows_stock', 'economy:varchar') }}
        union
        select distinct economy from {{ poc_optional_ref('unctad_merchandise_trade', 'economy:varchar') }}
        union
        select distinct economy from {{ poc_optional_ref('unctad_services_trade', 'economy:varchar') }}
    ) as c
    left join unctad_areas as a on c.economy = a.economy
    left join geo as g on a.candidate_key = g.geography_key
    where c.economy is not null
),

-- WHO data.who.int: areas by M49 code (unpadded) and type; countries through poc_m49_eu27 (the EU27 members),
-- World (GLOBAL 1) = WB:WLD. WHO regions and income groups have no Gold key (_X); the build stage builds only the
-- mapped areas. source_code = <type>:<code>, as region and country codes share one number space.
who_m49 as ({{ poc_m49_eu27() }}),

who as (
    select
        'who' as source_system,
        a.geo_type || ':' || a.geo_code_m49 as source_code,
        coalesce(g.geography_key, case when a.geo_type = 'GLOBAL' and a.geo_code_m49 = '1' then 'WB:WLD' end, '_X')
            as geography_key
    from (
        select distinct
            geo_type,
            geo_code_m49
        from {{ poc_optional_ref('who_observations', 'geo_type:varchar, geo_code_m49:varchar') }}
    ) as a
    left join who_m49 as m on a.geo_type = 'COUNTRY' and lpad(a.geo_code_m49, 3, '0') = m.m49
    left join geo as g on g.level_name = 'country' and m.iso2 = g.geography_key
),

-- UNESCO UIS: countries by ISO 3166-1 alpha-3 (dim_geography.iso3); regional units ('SDG: ...', 'WB: ...') have no
-- Gold key (_X), and the build stage builds only the EU27 members.
unesco_uis as (
    select
        'unesco_uis' as source_system,
        a.geo_unit as source_code,
        coalesce(g.geography_key, '_X') as geography_key
    from (
        select distinct
            geo_unit,
            is_country_code
        from {{ poc_optional_ref('unesco_uis_observations', 'geo_unit:varchar, is_country_code:boolean') }}
    ) as a
    left join geo as g on a.is_country_code and g.level_name = 'country' and a.geo_unit = g.iso3
),

-- FAOSTAT: FAO area codes; countries through their UN M49 code (poc_m49_eu27: the EU27 members), FAO aggregates
-- 5707 European Union (27) = EU27_2020 (current members, fixed composition) and 5000 World. Other areas have no
-- Gold key here (_X); the build stage builds only the mapped ones.
faostat_m49 as ({{ poc_m49_eu27() }}),

faostat as (
    select
        'faostat' as source_system,
        cast(a.area_code as varchar) as source_code,
        coalesce(
            g.geography_key,
            case a.area_code when 5707 then 'EUROSTAT:EU27_2020' when 5000 then 'WB:WLD' end,
            '_X'
        ) as geography_key
    from (
        select distinct
            area_code,
            area_code_m49
        from {{ poc_optional_ref('faostat_observations', 'area_code:integer, area_code_m49:varchar') }}
    ) as a
    left join faostat_m49 as m on a.area_code_m49 = m.m49 and a.area_code < 5000
    left join geo as g on g.level_name = 'country' and m.iso2 = g.geography_key
)

select
    m.source_system,
    m.source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    g.geography_key is not null as matched
from (
    {{ poc_geography_union() }}select * from unctad
    union all
    select * from who
    union all
    select * from unesco_uis
    union all
    select * from faostat
) as m
left join geo as g on m.geography_key = g.geography_key and g.geography_key <> '_X'
where m.source_code is not null
qualify row_number() over (partition by m.source_system, m.source_code order by g.geography_key nulls last) = 1
