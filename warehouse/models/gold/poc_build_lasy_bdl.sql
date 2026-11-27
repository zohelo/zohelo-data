{{ config(alias='build_lasy_bdl', tags=['gold_block'], pre_hook="install spatial; load spatial") }}
{# Gold build stage: Lasy Państwowe Bank Danych o Lasach (register PL-ENV-018), snapshot of the forest districts
   (nadleśnictwa) of the newest WFS download, an entity snapshot as build_pig_midas: one date = the download date
   (snapshot_date), frequency D. Block districts:
   - lasy_bdl:districts:districts:count - forest districts; Poland and the 16 voivodeships (is_total), and per
     regional directorate under Poland (breakdown rdlp = RDLP code as published, label its name);
   - lasy_bdl:districts:m2:area - planar area of the districts' territorial ranges in m² (EPSG:2180, computed from the
     geometry in Bronze: the layer has no area attribute), summed over the same areas and directorates;
   - lasy_bdl:districts:m2:district_area - the same area per district under Poland (breakdown district = district
     code region-inspectorate, label its name); no total row (the total is the area measure).
   A district counts in the voivodeship of the PRG gmina (silver.prg_units, EPSG:4258; first two digits of its TERYT
   code) whose boundary holds the point on its surface (always inside the district); its whole area goes there, it is
   not split at voivodeship borders. A point on a gmina border lies in two gminas: of one voivodeship it counts once,
   of two voivodeships it fails the build. A district outside every gmina counts for Poland only. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'lasy_bdl'
),

gminas as materialized (  -- small polygons parsed once; the bounding box narrows each point to a few candidates
    select
        teryt_code,
        geom,
        st_xmin(geom) as xmin,
        st_xmax(geom) as xmax,
        st_ymin(geom) as ymin,
        st_ymax(geom) as ymax
    from (
        select
            teryt_code,
            st_geomfromwkb(geometry_wkb) as geom
        from {{ ref('prg_units') }}
        where unit_level = 'gmina'
    ) as parsed
),

districts as (
    select
        d.district_code,
        d.region_code,
        d.name,
        r.name as region_name,
        d.area_m2,
        d.point_lon,
        d.point_lat,
        d.snapshot_date,
        d.source_version,
        d.raw_sha256
    from {{ ref('lasy_bdl_districts') }} as d
    left join {{ ref('lasy_bdl_regions') }} as r on d.region_code = r.region_code
),

candidates as (
    select
        d.district_code,
        d.point_lon,
        d.point_lat,
        g.teryt_code
    from districts as d
    inner join gminas as g
        on
            d.point_lon between g.xmin and g.xmax
            and d.point_lat between g.ymin and g.ymax
),

placed as (
    select
        k.district_code,
        min(substr(k.teryt_code, 1, 2)) as teryt_code,
        max(substr(k.teryt_code, 1, 2)) as teryt_code_max
    from candidates as k
    inner join gminas as g on k.teryt_code = g.teryt_code
    where st_contains(g.geom, st_point(k.point_lon, k.point_lat))
    group by k.district_code
),

areas as (
    select
        d.*,
        'PL' as area_code
    from districts as d
    union all
    select
        d.*,
        case
            when p.teryt_code <> p.teryt_code_max
                then error('build_lasy_bdl: district ' || p.district_code || ' in two voivodeships')
            else p.teryt_code
        end as area_code
    from districts as d
    inner join placed as p on d.district_code = p.district_code
),

counted as (
    select
        area_code,
        'districts' as unit_code,
        'count' as measure_code,
        '{}' as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        true as is_total,
        cast(count(*) as double) as value
    from areas
    group by area_code
    union all
    select
        area_code,
        'm2' as unit_code,
        'area' as measure_code,
        '{}' as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        true as is_total,
        sum(area_m2) as value
    from areas
    group by area_code
    union all
    select
        area_code,
        unit_code,
        measure_code,
        cast(json_object('rdlp', region_code) as varchar) as breakdown_json,
        cast(json_object('rdlp', region_name) as varchar) as breakdown_label_json,
        false as is_total,
        value
    from (
        select
            area_code,
            region_code,
            region_name,
            'districts' as unit_code,
            'count' as measure_code,
            cast(count(*) as double) as value
        from areas
        where area_code = 'PL'
        group by area_code, region_code, region_name
        union all
        select
            area_code,
            region_code,
            region_name,
            'm2' as unit_code,
            'area' as measure_code,
            sum(area_m2) as value
        from areas
        where area_code = 'PL'
        group by area_code, region_code, region_name
    ) as by_region
    union all
    select
        area_code,
        'm2' as unit_code,
        'district_area' as measure_code,
        cast(json_object('district', district_code) as varchar) as breakdown_json,
        cast(json_object('district', name) as varchar) as breakdown_label_json,
        false as is_total,
        area_m2 as value
    from areas
    where area_code = 'PL'
),

snapshot as (
    select
        max(snapshot_date) as state_date,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256,
        count(distinct raw_sha256) as files
    from districts
)

select
    'lasy_bdl' as source_system,
    'districts' as block_id,
    'Lasy Państwowe BDL forest districts' as block_name,
    'lasy_bdl:districts:' || c.unit_code || ':' || c.measure_code as measure_key,
    case c.measure_code
        when 'count' then 'Forest districts of the State Forests (BDL)'
        when 'area' then 'Area of the territorial ranges of the forest districts (BDL, planar EPSG:2180)'
        when 'district_area' then 'Area of the territorial range of a forest district (BDL, planar EPSG:2180)'
    end as measure_name,
    c.measure_code as measure_codes,
    c.unit_code,
    case c.unit_code when 'districts' then 'forest districts' else 'm²' end as unit_name,
    c.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    s.state_date as date_key,
    cast(s.state_date as varchar) as period_label,
    c.breakdown_json,
    c.breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.is_total,
    c.value,
    'ok' as observation_status_key,
    s.source_version,
    case
        when s.files > 1 then error('build_lasy_bdl: the block reads two Landing files')
        else s.raw_sha256
    end as raw_sha256
from counted as c
cross join snapshot as s
left join geo as g on c.area_code = g.source_code
