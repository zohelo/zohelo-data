{{ config(alias='build_gugik_orto', tags=['gold_block'], pre_hook="install spatial; load spatial") }}
{# Gold build stage: GUGiK orthophoto sheet index (register PL-ENV-004), coverage counts by data year from
   silver.gugik_orto_sheets (one row per index feature = sheet edition). Grain: measure_key x geography_key x
   breakdown x date_key, annual (date_key = 1 January of the feature type's data year). Rule: every feature counts for
   Poland ('PL': the national index); it also counts for the voivodeship that contains its footprint centroid
   (EPSG:2180, computed in Silver): the PRG gmina boundary (silver.prg_units, EPSG:4258, transformed to EPSG:2180) that
   contains the centroid gives the voivodeship, the first two digits of its TERYT code. Gminas, not the voivodeship
   polygons, because a test against a whole voivodeship boundary for each of 1.3 M points ran out of memory (Dev trial
   45824096302); the gminas of a voivodeship cover it exactly in PRG. A feature whose centroid lies in no gmina (sea,
   border strips) or that has no centroid counts for Poland only; a centroid in two voivodeships fails the build (no
   silent pick). Measures: sheet editions (index features) in total (pixel_class '_T', is_total) and by pixel-size
   class; the share of each class in the area's editions of the year (%). Pixel classes on the published pixel size in
   metres: le_0.05, 0.05-0.10 (above 0.05 up to 0.10), 0.10-0.25, gt_0.25, '_U' without a pixel size. Covered area is
   not given: editions of one year overlap (several deliveries, colours and pixel sizes of one sheet), so a sum of
   footprints overstates it. source_version is the newest year version used; raw_sha256 is the SHA-256 of the sorted
   list of the Landing pages used. #}
with sheets as (
    select
        feature_id,
        data_year,
        case
            when pixel_m is null then '_U'
            when pixel_m <= 0.05 then 'le_0.05'
            when pixel_m <= 0.10 then '0.05-0.10'
            when pixel_m <= 0.25 then '0.10-0.25'
            else 'gt_0.25'
        end as pixel_class,
        centroid_x_2180,
        centroid_y_2180,
        raw_sha256,
        source_version
    from {{ source('gugik_orto_entities', 'gugik_orto_sheets') }}
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
        feature_id,
        'PL' as geography_source_code
    from sheets
    union all
    select
        s.feature_id,
        case
            when v.voivodeships_found > 1
                then error('build_gugik_orto: feature ' || s.feature_id || ' in two voivodeships')
            else v.voivodeship_code
        end as geography_source_code
    from sheets as s
    inner join point_voivodeships as v on s.centroid_x_2180 = v.x and s.centroid_y_2180 = v.y
),

tagged as (
    select
        a.geography_source_code,
        s.data_year,
        s.pixel_class,
        s.raw_sha256,
        s.source_version
    from areas as a
    inner join sheets as s on a.feature_id = s.feature_id
),

counted as (
    select
        geography_source_code,
        data_year,
        false as is_total,
        pixel_class,
        count(*) as editions,
        max(source_version) as source_version,
        sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as raw_sha256
    from tagged
    group by geography_source_code, data_year, pixel_class
    union all
    select
        geography_source_code,
        data_year,
        true as is_total,
        '_T' as pixel_class,
        count(*) as editions,
        max(source_version) as source_version,
        sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as raw_sha256
    from tagged
    group by geography_source_code, data_year
),

observations as (
    select
        'editions' as measure,
        'map_sheets' as unit_code,
        geography_source_code,
        data_year,
        is_total,
        pixel_class,
        cast(editions as double) as value,
        source_version,
        raw_sha256
    from counted
    union all
    select
        'share_by_pixel_class' as measure,
        'pct' as unit_code,
        c.geography_source_code,
        c.data_year,
        false as is_total,
        c.pixel_class,
        round(100.0 * c.editions / t.editions, 2) as value,
        c.source_version,
        c.raw_sha256
    from counted as c
    inner join counted as t
        on c.geography_source_code = t.geography_source_code and c.data_year = t.data_year and t.is_total
    where not c.is_total
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gugik_orto'
)

select
    'gugik_orto' as source_system,
    'sheets' as block_id,
    'GUGiK orthophoto sheet index, editions by data year' as block_name,
    'gugik_orto:sheets:' || o.unit_code || ':' || o.measure as measure_key,
    case o.measure
        when 'editions' then 'Orthophoto sheet editions in the GUGiK index (sheets)'
        else 'Share of orthophoto sheet editions by pixel-size class (%)'
    end as measure_name,
    o.measure as measure_codes,
    o.unit_code,
    case o.unit_code when 'pct' then 'per cent' else 'map sheets' end as unit_name,
    o.geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.data_year, 1, 1) as date_key,
    cast(o.data_year as varchar) as period_label,
    '{"pixel_class": "' || o.pixel_class || '"}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from observations as o
left join geo as g on o.geography_source_code = g.source_code
