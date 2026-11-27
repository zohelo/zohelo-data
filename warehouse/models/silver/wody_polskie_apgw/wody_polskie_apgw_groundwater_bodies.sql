{# Wody Polskie aPGW (register PL-ENV-013, #3313): the groundwater bodies (JCWPd) of a plan cycle's geodatabase, one
   row per plan_cycle x jcwpd_code. Source: bronze.wody_polskie_apgw_attributes (newest copy of each entity), layer
   Jednolite_części_wód_podziemnych (code, number, EU code, area POWIERZCHN in km² as published) and
   Charakterystyka_JCWPd (several rows per body, one per water region x RZGW x water balance area: their published
   names are listed here, comma-separated, with the count of rows). Bronze has one row per feature x field, so each
   pivot reads exactly one value (max() over one row). A code found twice in the area layer gives two rows here and
   fails the unique test. river_basin_district_code: the code is 'GW' + the 4-digit river basin district code
   (RBD_MS_CD) + the body's number (GW6000126 = district 6000, number 126); it is taken only when the code equals
   exactly 'GW' || district || number, else NULL (fails not_null), and the completeness test checks that the
   district exists in the same geodatabase. Grain: plan_cycle x jcwpd_code. #}
with bronze as (
    select *
    from {{ source('wody_polskie_apgw', 'wody_polskie_apgw_attributes') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

attributes as (
    select
        entity,
        gdb_name,
        layer,
        feature_number,
        lower(field) as field,
        value,
        raw_sha256,
        source_version
    from bronze
),

bodies as (
    select
        entity,
        max(gdb_name) as gdb_name,
        max(raw_sha256) as raw_sha256,
        max(source_version) as source_version,
        max(value) filter (where field = 'kod_jcwpd') as jcwpd_code,
        max(value) filter (where field = 'nr_jcwpd') as jcwpd_number,
        max(value) filter (where field = 'kod_ue') as eu_code,
        max(value) filter (where field = 'powierzchn') as area_km2
    from attributes
    where layer = 'Jednolite_części_wód_podziemnych'
    group by entity, feature_number
),

characteristics as (
    select
        entity,
        feature_number,
        max(value) filter (where field = 'kod_jcwpd') as jcwpd_code,
        max(value) filter (where field = 'obsz_dorz') as river_basin_district,
        max(value) filter (where field = 'reg_wodny') as water_region,
        max(value) filter (where field = 'rzgw') as rzgw,
        max(value) filter (where field = 'obsz_bilan') as balance_area
    from attributes
    where layer = 'Charakterystyka_JCWPd'
    group by entity, feature_number
),

described as (
    select
        entity,
        jcwpd_code,
        count(*) as characteristic_rows,
        string_agg(distinct river_basin_district, ', ' order by river_basin_district) as river_basin_districts,
        count(distinct river_basin_district) as river_basin_district_names,
        string_agg(distinct water_region, ', ' order by water_region) as water_regions,
        string_agg(distinct rzgw, ', ' order by rzgw) as rzgws,
        string_agg(distinct balance_area, ', ' order by balance_area) as balance_areas
    from characteristics
    group by entity, jcwpd_code
)

select
    b.entity as plan_cycle,
    try_cast(strptime(regexp_extract(b.gdb_name, '_ver_([0-9]{8})', 1), '%Y%m%d') as date) as edition_date,
    b.jcwpd_code,
    try_cast(b.jcwpd_number as integer) as jcwpd_number,
    b.eu_code,
    try_cast(b.area_km2 as double) as area_km2,
    case
        when b.jcwpd_code = 'GW' || regexp_extract(b.jcwpd_code, '^GW([0-9]{4})', 1) || b.jcwpd_number
            then regexp_extract(b.jcwpd_code, '^GW([0-9]{4})', 1)
    end as river_basin_district_code,
    d.river_basin_districts,
    d.river_basin_district_names,
    d.water_regions,
    d.rzgws,
    d.balance_areas,
    coalesce(d.characteristic_rows, 0) as characteristic_rows,
    b.raw_sha256,
    b.source_version
from bodies as b
left join described as d on b.entity = d.entity and b.jcwpd_code = d.jcwpd_code
