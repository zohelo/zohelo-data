{# Wody Polskie aPGW (register PL-ENV-013, #3313): the planning units of a plan cycle's geodatabase, one row per
   plan_cycle x unit_kind x unit_code. Source: bronze.wody_polskie_apgw_attributes (newest copy of each entity),
   layers Obszary_dorzeczy (river_basin_district: RBD_MS_CD, RBD_EU_CD, Polish and English name, competent
   authorities), Regiony_wodne (water_region: RB_MS_CD, RB_EU_CD, name, its district RBD_MS_CD) and
   Regionalne_Zarządy_Gospodarki_Wodnej (rzgw: RZGW_MS_CD, RZGW_EU_CD, name, the districts and water regions it
   covers as published lists). area_km2 is the published AREA (km²; the districts' areas include the parts outside
   Poland that the plans cover). Names keep the published text except trailing white space (two water-region names
   end in a line break). Bronze has one row per feature x field, so each pivot reads exactly one value. Grain:
   plan_cycle x unit_kind x unit_code. #}
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
    where layer in ('Obszary_dorzeczy', 'Regiony_wodne', 'Regionalne_Zarządy_Gospodarki_Wodnej')
),

units as (
    select
        entity,
        layer,
        max(gdb_name) as gdb_name,
        max(raw_sha256) as raw_sha256,
        max(source_version) as source_version,
        max(value) filter (where field = case layer
            when 'Obszary_dorzeczy' then 'rbd_ms_cd'
            when 'Regiony_wodne' then 'rb_ms_cd'
            else 'rzgw_ms_cd'
        end) as unit_code,
        max(value) filter (where field = case layer
            when 'Obszary_dorzeczy' then 'rbd_eu_cd'
            when 'Regiony_wodne' then 'rb_eu_cd'
            else 'rzgw_eu_cd'
        end) as eu_code,
        max(value) filter (where field = case layer
            when 'Obszary_dorzeczy' then 'rbd_name_nl'
            when 'Regiony_wodne' then 'rb_name'
            else 'rzgw_name'
        end) as name_pl,
        max(value) filter (where layer = 'Obszary_dorzeczy' and field = 'rbd_name') as name_en,
        max(value) filter (where layer = 'Regiony_wodne' and field = 'rbd_ms_cd') as river_basin_district_code,
        max(value) filter (where layer = 'Regionalne_Zarządy_Gospodarki_Wodnej' and field = 'o_dorzeczy')
            as river_basin_district_codes,
        max(value) filter (where layer = 'Regionalne_Zarządy_Gospodarki_Wodnej' and field = 'gb_ms_cd')
            as water_region_codes,
        max(value) filter (where field = 'competent_prime') as competent_authority,
        max(value) filter (where field = 'competent_other') as other_authorities,
        max(value) filter (where field = 'area') as area_km2
    from attributes
    group by entity, layer, feature_number
)

select
    entity as plan_cycle,
    try_cast(strptime(regexp_extract(gdb_name, '_ver_([0-9]{8})', 1), '%Y%m%d') as date) as edition_date,
    case layer
        when 'Obszary_dorzeczy' then 'river_basin_district'
        when 'Regiony_wodne' then 'water_region'
        else 'rzgw'
    end as unit_kind,
    unit_code,
    eu_code,
    regexp_replace(name_pl, '\s+$', '') as name_pl,
    name_en,
    river_basin_district_code,
    river_basin_district_codes,
    water_region_codes,
    competent_authority,
    other_authorities,
    try_cast(area_km2 as double) as area_km2,
    raw_sha256,
    source_version
from units
