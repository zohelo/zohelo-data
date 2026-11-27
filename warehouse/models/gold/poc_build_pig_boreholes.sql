{{ config(alias='build_pig_boreholes', tags=['gold_block'], pre_hook="install spatial; load spatial") }}
{# Gold build stage: PIG-PIB CBDG boreholes (register PL-ENV-014), snapshot counts of the newest WFS download, an entity
   snapshot as other registers (pig_midas, nid_monuments): one date = the download date (snapshot_date), frequency D;
   the value is the number of boreholes the register holds on that date, not boreholes drilled in a period.
   Block boreholes, measure pig_boreholes:boreholes:boreholes:cbdg - boreholes in CBDG: the total (is_total, breakdown
   {}) and by drilling purpose (Cel_wiercenia, Polish as published; a borehole without one under '_U'; the producer's
   own 'brak danych' stays a category). Areas: Poland (every borehole, also those outside every gmina), the powiats
   and the voivodeships. A borehole is placed by its point (EPSG:4326 degrees from the WFS geometry) in the PRG gmina
   (silver.prg_units, EPSG:4258) whose boundary covers it; the powiat and voivodeship are the first four and two digits
   of the gmina's TERYT code (gminas, not the powiat or voivodeship outlines: cheaper polygons, the same codes). A point
   that falls in gminas of two powiats (on their border) is left out of the powiats, and of two voivodeships out of the
   voivodeships, never given to one of them; it still counts in Poland. The name fields of the WFS (Gmina, Powiat,
   Województwo) are not used (cross-check in the Dev trial). Borehole names stay out of Gold. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'pig_boreholes'
),

gminas as materialized (
    select
        teryt_code,
        st_geomfromwkb(geometry_wkb) as geom
    from {{ ref('prg_units') }}
    where unit_level = 'gmina'
),

boreholes as materialized (
    select
        id_cbdg,
        coalesce(drilling_purpose, '_U') as drilling_purpose,
        st_point(point_lon, point_lat) as pt,
        snapshot_date,
        source_version,
        raw_sha256
    from {{ ref('pig_boreholes_boreholes') }}
),

hits as (
    select
        b.id_cbdg,
        substr(g.teryt_code, 1, 4) as powiat_code,
        substr(g.teryt_code, 1, 2) as woj_code
    from boreholes as b
    inner join gminas as g on st_intersects(g.geom, b.pt)
),

placed as (  -- one row per borehole that falls in at least one gmina; a code only when all its gminas agree
    select
        id_cbdg,
        case when count(distinct powiat_code) = 1 then min(powiat_code) end as powiat_code,
        case when count(distinct woj_code) = 1 then min(woj_code) end as woj_code
    from hits
    group by id_cbdg
),

areas as (
    select
        b.id_cbdg,
        b.drilling_purpose,
        'PL' as area_code
    from boreholes as b
    union all
    select
        b.id_cbdg,
        b.drilling_purpose,
        p.woj_code as area_code
    from boreholes as b
    inner join placed as p on b.id_cbdg = p.id_cbdg
    where p.woj_code is not null
    union all
    select
        b.id_cbdg,
        b.drilling_purpose,
        p.powiat_code as area_code
    from boreholes as b
    inner join placed as p on b.id_cbdg = p.id_cbdg
    where p.powiat_code is not null
),

counted as (
    select
        area_code,
        '{}' as breakdown_json,
        true as is_total,
        cast(count(*) as double) as value
    from areas
    group by area_code
    union all
    select
        area_code,
        cast(json_object('drilling_purpose', drilling_purpose) as varchar) as breakdown_json,
        false as is_total,
        cast(count(*) as double) as value
    from areas
    group by area_code, drilling_purpose
),

snapshot as (
    select
        max(snapshot_date) as state_date,
        max(source_version) as source_version,
        sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256)) as raw_sha256,
        count(distinct snapshot_date) as dates
    from boreholes
)

select
    'pig_boreholes' as source_system,
    'boreholes' as block_id,
    'PIG-PIB CBDG boreholes' as block_name,
    'pig_boreholes:boreholes:boreholes:cbdg' as measure_key,
    'Boreholes in the Central Geological Database (CBDG)' as measure_name,
    'cbdg' as measure_codes,
    'boreholes' as unit_code,
    'boreholes' as unit_name,
    c.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    case when s.dates > 1 then error('build_pig_boreholes: Silver holds two snapshot dates') else s.state_date end
        as date_key,
    cast(s.state_date as varchar) as period_label,
    c.breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.is_total,
    c.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from counted as c
cross join snapshot as s
left join geo as g on c.area_code = g.source_code
