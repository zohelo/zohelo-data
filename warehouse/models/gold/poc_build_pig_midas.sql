{{ config(alias='build_pig_midas', tags=['gold_block'], pre_hook="install spatial; load spatial") }}
{# Gold build stage: PIG-PIB MIDAS (register PL-ENV-015), snapshot counts of the newest WFS download, an entity
   snapshot as other registers (nid_monuments, GLEIF): one date per layer = the download date (snapshot_date), frequency
   D. Areas: Poland (every contour, also those at sea outside every voivodeship) and the 16 voivodeships; a contour
   counts in the voivodeship of the PRG gmina (silver.prg_units, EPSG:4258; the first two digits of its TERYT code)
   whose boundary holds the point on its surface (point_lon/point_lat, always inside the contour); gminas, not the
   voivodeship outlines, because a full-resolution voivodeship polygon parsed per point exhausts memory. A point on
   a gmina border lies in two gminas: of one voivodeship it is kept once; of two voivodeships it fails the build.
   - block deposits, pig_midas:deposits:deposits:documented - documented mineral deposits: distinct deposit numbers of
     the layer deposits; breakdown mineral = the contour's mineral (Kopalina, as published; the code Kod is not one
     to one with it: KN holds 11 minerals), so a deposit with two minerals counts under each; the total (is_total,
     breakdown {}) counts each deposit once; a deposit with contours in two
     voivodeships counts in each, once in Poland. Removed deposits (archival layer) are left out.
   - block mining_areas, pig_midas:mining_areas:mining_areas:current - current mining areas: contours (ID_konturu) of
     the layer mining_areas with status 'aktualny' (a record the producer has not completed, without status, is left
     out: 1 on 26 Nov 2026); pig_midas:mining_areas:m2:legal_area - their legal area in m2 as published (Powierzchnia
     OG), summed; when a contour of the area has no legal area the sum covers the others and the row is flagged
     'definition_differs' (never counted as 0). Abolished areas and the mining terrains are left out. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'pig_midas'
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

contours as (
    select
        'deposits' as block_id,
        layer || ':' || objectid as contour_key,
        cast(deposit_number as varchar) as entity_key,
        mineral_code,
        mineral,
        cast(null as double) as legal_area_m2,
        point_lon,
        point_lat,
        snapshot_date,
        source_version,
        raw_sha256
    from {{ ref('pig_midas_deposits') }}
    where layer = 'deposits'
    union all
    select
        'mining_areas' as block_id,
        layer || ':' || contour_id as contour_key,
        cast(contour_id as varchar) as entity_key,
        null as mineral_code,
        null as mineral,
        legal_area_m2,
        point_lon,
        point_lat,
        snapshot_date,
        source_version,
        raw_sha256
    from {{ ref('pig_midas_mining_areas') }}
    where layer = 'mining_areas' and status = 'aktualny'
),

candidates as (
    select
        c.contour_key,
        c.point_lon,
        c.point_lat,
        g.teryt_code
    from contours as c
    inner join gminas as g
        on
            c.point_lon between g.xmin and g.xmax
            and c.point_lat between g.ymin and g.ymax
),

placed as (
    select
        k.contour_key,
        substr(k.teryt_code, 1, 2) as teryt_code,
        min(substr(k.teryt_code, 1, 2)) over (partition by k.contour_key)
        <> max(substr(k.teryt_code, 1, 2)) over (partition by k.contour_key) as in_two_voivodeships
    from candidates as k
    inner join gminas as g on k.teryt_code = g.teryt_code
    where st_contains(g.geom, st_point(k.point_lon, k.point_lat))
    qualify row_number() over (partition by k.contour_key order by k.teryt_code) = 1
),

areas as (
    select
        c.*,
        'PL' as area_code
    from contours as c
    union all
    select
        c.*,
        case
            when p.in_two_voivodeships
                then error('build_pig_midas: contour ' || p.contour_key || ' in two voivodeships')
            else p.teryt_code
        end as area_code
    from contours as c
    inner join placed as p on c.contour_key = p.contour_key
),

counted as (
    select
        block_id,
        area_code,
        block_id as unit_code,
        case block_id when 'deposits' then 'documented' else 'current' end as measure_code,
        '{}' as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        true as is_total,
        cast(count(distinct entity_key) as double) as value,
        'ok' as observation_status_key
    from areas
    group by block_id, area_code
    union all
    select
        block_id,
        area_code,
        'deposits' as unit_code,
        'documented' as measure_code,
        cast(json_object('mineral', mineral) as varchar) as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        false as is_total,
        cast(count(distinct entity_key) as double) as value,
        'ok' as observation_status_key
    from areas
    where block_id = 'deposits'
    group by block_id, area_code, mineral
    union all
    select
        block_id,
        area_code,
        'm2' as unit_code,
        'legal_area' as measure_code,
        '{}' as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        true as is_total,
        sum(legal_area_m2) as value,
        case when count(*) > count(legal_area_m2) then 'definition_differs' else 'ok' end as observation_status_key
    from areas
    where block_id = 'mining_areas'
    group by block_id, area_code
    having count(legal_area_m2) > 0
),

snapshot as (
    select
        block_id,
        max(snapshot_date) as state_date,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256,
        count(distinct raw_sha256) as files
    from contours
    group by block_id
)

select
    'pig_midas' as source_system,
    c.block_id,
    case c.block_id
        when 'deposits' then 'PIG-PIB MIDAS documented mineral deposits'
        else 'PIG-PIB MIDAS mining areas'
    end as block_name,
    'pig_midas:' || c.block_id || ':' || c.unit_code || ':' || c.measure_code as measure_key,
    case c.measure_code
        when 'documented' then 'Documented mineral deposits (MIDAS)'
        when 'current' then 'Current mining areas (MIDAS contours)'
        when 'legal_area' then 'Legal area of current mining areas (MIDAS)'
    end as measure_name,
    c.measure_code as measure_codes,
    c.unit_code,
    case c.unit_code when 'deposits' then 'deposits' when 'mining_areas' then 'mining areas' else 'm²' end as unit_name,
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
    c.observation_status_key,
    s.source_version,
    case
        when s.files > 1 then error('build_pig_midas: block ' || c.block_id || ' reads two Landing files')
        else s.raw_sha256
    end as raw_sha256
from counted as c
inner join snapshot as s on c.block_id = s.block_id
left join geo as g on c.area_code = g.source_code
