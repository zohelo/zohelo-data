{{ config(alias='build_gugik_orto', tags=['gold_block'], pre_hook="install spatial; load spatial") }}
{# Gold build stage: GUGiK orthophoto sheet index (register PL-ENV-004), coverage counts by data year from
   silver.gugik_orto_sheets (one row per index feature = sheet edition). Grain: measure_key x geography_key x
   breakdown x date_key, annual (date_key = 1 January of the feature type's data year). Rule: every feature counts for
   Poland ('PL': the national index); it also counts for the voivodeship whose PRG boundary (silver.prg_units, EPSG:4258,
   transformed to EPSG:2180) contains its footprint centroid (EPSG:2180, computed in Silver). A feature whose centroid
   lies in no voivodeship (sea, border strips) or that has no centroid counts for Poland only; one placed in two fails
   the build (no silent pick). Measures: sheet editions (index features) in total (pixel_class '_T', is_total) and by
   pixel-size class; the share of each class in the area's editions of the year (%). Pixel classes on the published
   pixel size in metres: le_0.05, 0.05-0.10 (above 0.05 up to 0.10), 0.10-0.25, gt_0.25, '_U' without a pixel size.
   Covered area is not given: editions of one year overlap (several deliveries, colours and pixel sizes of one sheet),
   so a sum of footprints overstates it. source_version is the newest year version used; raw_sha256 is the SHA-256 of
   the sorted list of the Landing pages used. #}
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

voivodeships as (
    select
        teryt_code,
        st_transform(st_geomfromwkb(geometry_wkb), 'EPSG:4258', 'EPSG:2180', always_xy := true) as geom
    from {{ ref('prg_units') }}
    where unit_level = 'wojewodztwo'
),

placed as (
    select
        s.feature_id,
        v.teryt_code,
        count(*) over (partition by s.feature_id) as voivodeships_found
    from sheets as s
    inner join voivodeships as v on st_contains(v.geom, st_point(s.centroid_x_2180, s.centroid_y_2180))
    where s.centroid_x_2180 is not null
),

areas as (
    select
        feature_id,
        'PL' as geography_source_code
    from sheets
    union all
    select
        feature_id,
        case
            when voivodeships_found > 1 then error('build_gugik_orto: feature ' || feature_id || ' in two voivodeships')
            else teryt_code
        end as geography_source_code
    from placed
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
