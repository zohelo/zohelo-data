{{ config(alias='build_gugik_nmt', tags=['gold_block'], pre_hook="install spatial; load spatial") }}
{# Gold build stage: GUGiK elevation sheet indexes (register PL-ENV-005), coverage counts by data year from
   silver.gugik_nmt_sheets (one row per index feature = a sheet edition in one file format, in one of six services:
   NMT terrain model, NMPT surface model, LIDAR point cloud, each in the EVRF2007 and the KRON86 index). Grain:
   measure_key x geography_key x breakdown x date_key, annual (date_key = 1 January of the feature type's data year).
   One measure per service: the products are different surfaces and the two vertical datums are separate indexes, so
   nothing is summed across them. Rule: every feature counts for Poland ('PL': the national index); it also counts
   for the voivodeship that contains its footprint centroid (EPSG:2180, computed in Silver): the PRG gmina boundary
   (silver.prg_units, EPSG:4258, transformed to EPSG:2180) that contains the centroid gives the voivodeship, the
   first two digits of its TERYT code (gminas, not the voivodeship polygons: a test against whole voivodeship
   boundaries for 1.3 M orthophoto sheets ran out of memory, Dev trial 45824096302; the gminas of a voivodeship cover
   it exactly in PRG). A feature whose centroid lies in no gmina (sea, border strips) or that has no centroid counts
   for Poland only; a centroid in two voivodeships fails the build (no silent pick). A sheet edition is a distinct
   sheet id x date x spatial characteristic of a service's year: the index lists one feature per file format (an NMT
   sheet as ARC/INFO ASCII GRID and as ASCII XYZ GRID, Dev probe 45814802349), and formats are not counted twice.
   Measures: sheet editions in total (class '_T', is_total) and by spatial characteristic as published (grid size of
   NMT/NMPT, point density of LIDAR; '_U' when empty), and the share of each characteristic in the service's editions
   of the area and year (%). Covered area is not given: editions overlap, so a sum of footprints overstates it.
   source_version is the newest version used; raw_sha256 is the SHA-256 of the sorted list of the Landing pages used. #}
with sheets as (
    select
        feature_id,
        product || '_' || service_datum as service,
        sheet_id || '|' || coalesce(cast(acquisition_date as varchar), '') as edition,
        data_year,
        coalesce(nullif(trim(spatial_characteristic), ''), '_U') as class,
        centroid_x_2180,
        centroid_y_2180,
        raw_sha256,
        source_version
    from {{ source('gugik_nmt_entities', 'gugik_nmt_sheets') }}
),

gminas as (
    select
        substr(teryt_code, 1, 2) as voivodeship_code,
        geom,
        st_xmin(geom) as x_min,
        st_xmax(geom) as x_max,
        st_ymin(geom) as y_min,
        st_ymax(geom) as y_max
    from (
        select
            teryt_code,
            st_transform(st_geomfromwkb(geometry_wkb), 'EPSG:4258', 'EPSG:2180', always_xy := true) as geom
        from {{ ref('prg_units') }}
        where unit_level = 'gmina'
    ) as g
),

points as (
    select distinct
        centroid_x_2180 as x,
        centroid_y_2180 as y
    from sheets
    where centroid_x_2180 is not null
),

point_voivodeships as (
    select
        p.x,
        p.y,
        min(g.voivodeship_code) as voivodeship_code,
        count(distinct g.voivodeship_code) as voivodeships_found
    from points as p
    inner join gminas as g
        on
            p.x between g.x_min and g.x_max
            and p.y between g.y_min and g.y_max
            and st_contains(g.geom, st_point(p.x, p.y))
    group by p.x, p.y
),

areas as (
    select
        service,
        feature_id,
        'PL' as geography_source_code
    from sheets
    union all
    select
        s.service,
        s.feature_id,
        case
            when v.voivodeships_found > 1
                then error('build_gugik_nmt: feature ' || s.service || ' ' || s.feature_id || ' in two voivodeships')
            else v.voivodeship_code
        end as geography_source_code
    from sheets as s
    inner join point_voivodeships as v on s.centroid_x_2180 = v.x and s.centroid_y_2180 = v.y
),

tagged as (
    select
        a.geography_source_code,
        s.service,
        s.data_year,
        s.class,
        s.edition,
        s.raw_sha256,
        s.source_version
    from areas as a
    inner join sheets as s on a.service = s.service and a.feature_id = s.feature_id
),

counted as (
    select
        geography_source_code,
        service,
        data_year,
        false as is_total,
        class,
        count(distinct edition || '|' || class) as editions,
        max(source_version) as source_version,
        sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as raw_sha256
    from tagged
    group by geography_source_code, service, data_year, class
    union all
    select
        geography_source_code,
        service,
        data_year,
        true as is_total,
        '_T' as class,
        count(distinct edition || '|' || class) as editions,
        max(source_version) as source_version,
        sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as raw_sha256
    from tagged
    group by geography_source_code, service, data_year
),

observations as (
    select
        service || '_editions' as measure,
        'map_sheets' as unit_code,
        geography_source_code,
        service,
        data_year,
        is_total,
        class,
        cast(editions as double) as value,
        source_version,
        raw_sha256
    from counted
    union all
    select
        c.service || '_share_by_class' as measure,
        'pct' as unit_code,
        c.geography_source_code,
        c.service,
        c.data_year,
        false as is_total,
        c.class,
        round(100.0 * c.editions / t.editions, 2) as value,
        c.source_version,
        c.raw_sha256
    from counted as c
    inner join counted as t
        on
            c.geography_source_code = t.geography_source_code
            and c.service = t.service
            and c.data_year = t.data_year
            and t.is_total
    where not c.is_total
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gugik_nmt'
)

select
    'gugik_nmt' as source_system,
    'sheets' as block_id,
    'GUGiK elevation sheet indexes (NMT, NMPT, LIDAR), editions by data year' as block_name,
    'gugik_nmt:sheets:' || o.unit_code || ':' || o.measure as measure_key,
    case split_part(o.service, '_', 1)
        when 'nmt' then 'NMT (terrain model)'
        when 'nmpt' then 'NMPT (surface model)'
        else 'LIDAR point cloud'
    end
    || ' sheet editions, ' || upper(split_part(o.service, '_', 2)) || ' index'
    || case o.unit_code when 'pct' then ', share by spatial characteristic (%)' else ' (sheets)' end as measure_name,
    o.measure as measure_codes,
    o.unit_code,
    case o.unit_code when 'pct' then 'per cent' else 'map sheets' end as unit_name,
    o.geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.data_year, 1, 1) as date_key,
    cast(o.data_year as varchar) as period_label,
    cast(json_object('spatial_characteristic', o.class) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from observations as o
left join geo as g on o.geography_source_code = g.source_code
