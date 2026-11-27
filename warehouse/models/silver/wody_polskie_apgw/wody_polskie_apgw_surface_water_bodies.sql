{# Wody Polskie aPGW (register PL-ENV-013, #3313): the surface water bodies (JCWP) of a plan cycle's geodatabase, one
   row per plan_cycle x jcwp_code. Source: bronze.wody_polskie_apgw_attributes (newest copy of each entity), layers
   Charakterystyka_JCWP (every JCWP: name, type, river basin district, water region, RZGW, RDOŚ, voivodeships, powiats
   and gminas with their TERYT codes, groundwater bodies, status and risk assessments as published), Status_JCWP
   (natural / heavily modified / artificial) and the area layers of the lake, transitional, coastal and reservoir
   JCWP (POW_JCWP, km², as published; the river JCWP are lines and have no area). Bronze has one row per feature x
   field, so each pivot below reads exactly one value (max() over one row, not a choice between rows). A code
   found twice in a layer gives two rows here and fails the unique test (never a silent pick).
   Derived, each with a guard: category from the code prefix (RW river, LW lake, TW transitional, CW coastal; any
   other prefix leaves it NULL and fails not_null); is_reservoir = the code is in the reservoir layer; the river
   basin district code (RBD_MS_CD) by the district's name in the same geodatabase's Obszary_dorzeczy layer (exact,
   case-insensitive; an unmatched name leaves it NULL and fails not_null); voivodeship_codes = the TERYT codes in
   brackets of the published list ('mazowieckie (14); łódzkie (10)' → '14,10'), with voivodeship_parts the count of
   ';'-separated parts (the completeness test fails when the two counts differ). '-909' and '-907' in the status
   columns are the publisher's "not assessed" / "not applicable" markers, kept as published text. edition_date from
   the geodatabase folder name (Geobaza_2aPGW_ver_20230915.gdb → 2023-09-15). Grain: plan_cycle x jcwp_code. #}
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

characteristics as (
    select
        entity,
        max(gdb_name) as gdb_name,
        max(raw_sha256) as raw_sha256,
        max(source_version) as source_version,
        max(value) filter (where field = 'kod_jcwp') as jcwp_code,
        max(value) filter (where field = 'nazwa_jcwp') as jcwp_name,
        max(value) filter (where field = 'typ_jcwp') as jcwp_type,
        max(value) filter (where field = 'jcwp_zmian') as change_since_previous_plan,
        max(value) filter (where field = 'ms_kod_apg') as previous_plan_codes,
        max(value) filter (where field = 'dlug_pow') as length_or_area,
        max(value) filter (where field = 'pow_zl') as catchment_area_km2,
        max(value) filter (where field = 'obsz_dorz') as river_basin_district,
        max(value) filter (where field = 'reg_wodny') as water_region,
        max(value) filter (where field = 'rzgw') as rzgw,
        max(value) filter (where field = 'rdos') as rdos,
        max(value) filter (where field = 'wojewodz') as voivodeships,
        max(value) filter (where field = 'powiat') as powiats,
        max(value) filter (where field = 'gmina') as gminas,
        max(value) filter (where field = 'kod_jcwpd') as groundwater_body_codes,
        max(value) filter (where field = 'kod_ppk_s') as monitoring_point_codes_status,
        max(value) filter (where field = 'kod_ppk_n') as monitoring_point_codes_new,
        max(value) filter (where field = 'st_eko') as ecological_status,
        max(value) filter (where field = 'st_chem') as chemical_status,
        max(value) filter (where field = 'ocena_stan') as overall_status,
        max(value) filter (where field = 'oc_ryz') as risk_assessment,
        max(value) filter (where field = 'zl') as cross_border,
        max(value) filter (where field = 'rk') as reference_conditions,
        max(value) filter (where field = 'och_inspire') as protected_area_codes,
        max(value) filter (where field = 'oc_gat_gos') as protected_fish_species,
        max(value) filter (where field = 'bud_reg') as has_regulating_structures,
        max(value) filter (where field = 'z_dzialan') as has_measures,
        max(value) filter (where field = 'z_dz_drozn') as continuity_measures
    from attributes
    where layer = 'Charakterystyka_JCWP'
    group by entity, feature_number
),

statuses as (
    select
        entity,
        max(value) filter (where field = 'kod_jcwp') as jcwp_code,
        max(value) filter (where field = 'status_os') as designation
    from attributes
    where layer = 'Status_JCWP'
    group by entity, feature_number
),

areas as (
    select
        entity,
        layer,
        max(value) filter (where field = 'ms_kod') as jcwp_code,
        max(value) filter (where field = 'eu_kod') as eu_code,
        max(value) filter (where field = 'pow_jcwp') as area_km2
    from attributes
    where
        layer in (
            'Jednolite_Części_Wód_Powierzchniowych_Jeziornych',
            'Jednolite_Części_Wód_Powierzchniowych_Przejściowych',
            'Jednolite_Części_Wód_Powierzchniowych_Przybrzeżnych',
            'Jednolite_Części_Wód_Powierzchniowych_Zbiornikowych'
        )
    group by entity, layer, feature_number
),

districts as (
    select
        entity,
        max(value) filter (where field = 'rbd_ms_cd') as rbd_code,
        max(value) filter (where field = 'rbd_name_nl') as rbd_name
    from attributes
    where layer = 'Obszary_dorzeczy'
    group by entity, feature_number
)

select
    c.entity as plan_cycle,
    try_cast(strptime(regexp_extract(c.gdb_name, '_ver_([0-9]{8})', 1), '%Y%m%d') as date) as edition_date,
    c.jcwp_code,
    a.eu_code,
    c.jcwp_name,
    case left(c.jcwp_code, 2)
        when 'RW' then 'river'
        when 'LW' then 'lake'
        when 'TW' then 'transitional'
        when 'CW' then 'coastal'
    end as category,
    coalesce(a.layer = 'Jednolite_Części_Wód_Powierzchniowych_Zbiornikowych', false) as is_reservoir,
    c.jcwp_type,
    s.designation,
    c.change_since_previous_plan,
    c.previous_plan_codes,
    try_cast(a.area_km2 as double) as area_km2,
    try_cast(c.length_or_area as double) as length_or_area,
    try_cast(c.catchment_area_km2 as double) as catchment_area_km2,
    c.river_basin_district,
    d.rbd_code as river_basin_district_code,
    c.water_region,
    c.rzgw,
    c.rdos,
    c.voivodeships,
    array_to_string(regexp_extract_all(c.voivodeships, '\(([0-9]{2})\)', 1), ',') as voivodeship_codes,
    len(string_split(c.voivodeships, ';')) as voivodeship_parts,
    c.powiats,
    c.gminas,
    c.groundwater_body_codes,
    c.monitoring_point_codes_status,
    c.monitoring_point_codes_new,
    c.ecological_status,
    c.chemical_status,
    c.overall_status,
    c.risk_assessment,
    c.cross_border,
    c.reference_conditions,
    c.protected_area_codes,
    c.protected_fish_species,
    c.has_regulating_structures,
    c.has_measures,
    c.continuity_measures,
    c.raw_sha256,
    c.source_version
from characteristics as c
left join statuses as s on c.entity = s.entity and c.jcwp_code = s.jcwp_code
left join areas as a on c.entity = a.entity and c.jcwp_code = a.jcwp_code
left join districts as d on c.entity = d.entity and lower(c.river_basin_district) = lower(d.rbd_name)
