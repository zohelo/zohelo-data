{{ config(alias='build_wody_polskie_apgw', tags=['gold_block']) }}
{# Gold build stage: Wody Polskie water-management planning data (register PL-ENV-013, #3313), block 'water_bodies',
   from silver.wody_polskie_apgw_surface_water_bodies, silver.wody_polskie_apgw_groundwater_bodies and (district
   names only) silver.wody_polskie_apgw_planning_units. Fact kind: reference-data counts of a planning register (an
   entity snapshot of the plan cycle), not observations: the number of water bodies the plan designates and the sum
   of their published areas. Annual, date_key = 1 January of the edition year of the plan cycle's geodatabase
   (Geobaza_2aPGW_ver_20230915 → 2023-01-01), never the download time; one frequency per measure.
   Measures (category in the measure, never summed across categories):
   wody_polskie_apgw:water_bodies:water_bodies:surface.<all|river|lake|transitional|coastal> (JCWP; category from the
   code prefix RW/LW/TW/CW, the 45 reservoir bodies are RW), …:water_bodies:groundwater (JCWPd),
   …:km2:surface_area.<lake|transitional|coastal> and …:km2:groundwater_area (sum of the published areas, written only
   when every body of the group has one; river bodies are lines and have no area, so there is no river or 'all'
   area). Breakdown: the river basin district (RBD_MS_CD, label its Polish name) on Poland's rows; each body belongs to
   one district, so the districts sum to the total. Areas: Poland ('PL', every body of the plan, wholly or partly in
   Poland) and, for the surface counts only, each voivodeship that the body's published voivodeship list names
   (TERYT code in brackets): a body in two voivodeships counts in both, so the voivodeships do not sum to Poland, as
   the measure name says. Groundwater bodies carry no voivodeship list: Poland only. #}
with surface as (
    select
        plan_cycle,
        year(edition_date) as edition_year,
        category,
        river_basin_district_code,
        voivodeship_codes,
        area_km2,
        raw_sha256,
        source_version
    from {{ ref('wody_polskie_apgw_surface_water_bodies') }}
),

surface_all as (
    select * from surface
    union all
    select * replace ('all' as category) from surface
),

groundwater as (
    select
        plan_cycle,
        year(edition_date) as edition_year,
        river_basin_district_code,
        area_km2,
        raw_sha256,
        source_version
    from {{ ref('wody_polskie_apgw_groundwater_bodies') }}
),

district_names as (
    select
        plan_cycle,
        unit_code,
        name_pl
    from {{ ref('wody_polskie_apgw_planning_units') }}
    where unit_kind = 'river_basin_district'
),

surface_counts as (
    -- Poland in total
    select
        plan_cycle,
        edition_year,
        category as code,
        'PL' as geo_code,
        cast(null as varchar) as district,
        count(*) as bodies,
        count(area_km2) as with_area,
        sum(area_km2) as area,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from surface_all
    group by plan_cycle, edition_year, category
    union all
    -- Poland by river basin district (not_null in Silver: every body has one)
    select
        plan_cycle,
        edition_year,
        category as code,
        'PL' as geo_code,
        river_basin_district_code as district,
        count(*) as bodies,
        count(area_km2) as with_area,
        sum(area_km2) as area,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from surface_all
    group by plan_cycle, edition_year, category, river_basin_district_code
    union all
    -- each voivodeship the body's published list names
    select
        s.plan_cycle,
        s.edition_year,
        s.category as code,
        v.code as geo_code,
        cast(null as varchar) as district,
        count(*) as bodies,
        count(s.area_km2) as with_area,
        sum(s.area_km2) as area,
        max(s.source_version) as source_version,
        max(s.raw_sha256) as raw_sha256
    from surface_all as s
    cross join unnest(string_split(s.voivodeship_codes, ',')) as v (code)
    group by s.plan_cycle, s.edition_year, s.category, v.code
),

groundwater_counts as (
    select
        plan_cycle,
        edition_year,
        'groundwater' as code,
        'PL' as geo_code,
        cast(null as varchar) as district,
        count(*) as bodies,
        count(area_km2) as with_area,
        sum(area_km2) as area,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from groundwater
    group by plan_cycle, edition_year
    union all
    select
        plan_cycle,
        edition_year,
        'groundwater' as code,
        'PL' as geo_code,
        river_basin_district_code as district,
        count(*) as bodies,
        count(area_km2) as with_area,
        sum(area_km2) as area,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from groundwater
    group by plan_cycle, edition_year, river_basin_district_code
),

geo_map as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'wody_polskie_apgw'
),

measures as (
    select
        *,
        'water_bodies' as unit_code,
        'surface.' || code as measure_codes,
        cast(bodies as double) as value
    from surface_counts
    union all
    select
        *,
        'km2' as unit_code,
        'surface_area.' || code as measure_codes,
        area as value
    from surface_counts
    where code in ('lake', 'transitional', 'coastal') and geo_code = 'PL' and with_area = bodies
    union all
    select
        *,
        'water_bodies' as unit_code,
        'groundwater' as measure_codes,
        cast(bodies as double) as value
    from groundwater_counts
    union all
    select
        *,
        'km2' as unit_code,
        'groundwater_area' as measure_codes,
        area as value
    from groundwater_counts
    where with_area = bodies
)

select
    'wody_polskie_apgw' as source_system,
    'water_bodies' as block_id,
    'Wody Polskie aPGW: water bodies of the river basin management plans' as block_name,
    'wody_polskie_apgw:water_bodies:' || m.unit_code || ':' || m.measure_codes as measure_key,
    case
        when m.measure_codes = 'groundwater' then 'Groundwater bodies (JCWPd) of the river basin management plan'
        when m.measure_codes = 'groundwater_area'
            then 'Area of the groundwater bodies (JCWPd) of the river basin management plan, as published'
        when m.unit_code = 'km2'
            then concat('Area of the ', m.code, ' surface water bodies (JCWP) of the plan, as published')
        when m.code = 'all'
            then 'Surface water bodies (JCWP) of the river basin management plan, wholly or partly in the area'
        else concat('Surface water bodies (JCWP) of the plan, ', m.code, ' category, wholly or partly in the area')
    end as measure_name,
    m.measure_codes,
    m.unit_code,
    case m.unit_code when 'km2' then 'km²' else 'water bodies' end as unit_name,
    m.geo_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(m.edition_year, 1, 1) as date_key,
    cast(m.edition_year as varchar) as period_label,
    case
        when m.district is null then '{}'
        else cast(json_object('river_basin_district', m.district) as varchar)
    end as breakdown_json,
    case
        when m.district is null then cast(null as varchar)
        else cast(json_object('river_basin_district', coalesce(d.name_pl, m.district)) as varchar)
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    m.district is null as is_total,
    m.value,
    'ok' as observation_status_key,
    m.source_version,
    m.raw_sha256
from measures as m
left join geo_map as g on m.geo_code = g.source_code
left join district_names as d on m.plan_cycle = d.plan_cycle and m.district = d.unit_code
