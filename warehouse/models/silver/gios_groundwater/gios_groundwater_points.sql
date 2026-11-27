{# GIOŚ groundwater quality monitoring (register PL-ENV-012, #2870): the monitoring points as each year's result files
   describe them (bronze.gios_groundwater_records, record_kind 'sample', newest copy of each entity), one row per
   point_key x survey_year (point_key as in gios_groundwater_results: MONBADA number, else 'IDM:<ID Monitoring>').
   A point's samples of one year (several dates, the inorganic and the organic file) must agree on each attribute:
   an attribute with one value is kept as published; one with several values in a year is left NULL and named in
   conflicting_attributes (never a chosen value). The voivodeship must be single (test of the model's yml) because
   Gold places the point by it. Kept as published: point numbers (MONBADA, ID Monitoring, SOH/SOBWP, CBDH), the EU
   point identifiers and groundwater bodies (JCWPd) of the 161, 172 and 174 divisions, the PUWG 1992 coordinates
   (EPSG:2180; X is the easting in these files: Świnoujście X 185,897, Y 678,640), voivodeship, powiat, gmina and
   locality names (no TERYT codes are published), river basin, RZGW, stratigraphy, depth to the aquifer top and the
   screened interval, water table, aquifer medium, point kind and land use. raw_sha256 and source_version list every
   file the point's samples of the year come from (comma-separated). Grain: point_key x survey_year. #}
with records as (
    select *
    from {{ source('gios_groundwater', 'gios_groundwater_records') }}
    where record_kind = 'sample'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

samples as (
    select
        *,
        coalesce(regexp_replace(point_monbada, '\.0$', ''), 'IDM:' || regexp_replace(point_id_monitoring, '\.0$', ''))
            as point_key,
        cast(left(entity, 4) as integer) as survey_year
    from records
)

select
    point_key,
    survey_year,
    case when count(distinct point_monbada) = 1 then min(point_monbada) end as point_monbada,
    case when count(distinct point_id_monitoring) = 1 then min(point_id_monitoring) end as point_id_monitoring,
    case when count(distinct point_soh) = 1 then min(point_soh) end as point_soh,
    case when count(distinct point_cbdh) = 1 then min(point_cbdh) end as point_cbdh,
    case when count(distinct point_eu_id_161) = 1 then min(point_eu_id_161) end as point_eu_id_161,
    case when count(distinct jcwpd161_number) = 1 then min(jcwpd161_number) end as jcwpd161_number,
    case when count(distinct jcwpd161_code) = 1 then min(jcwpd161_code) end as jcwpd161_code,
    case when count(distinct point_eu_id_172) = 1 then min(point_eu_id_172) end as point_eu_id_172,
    case when count(distinct jcwpd172_number) = 1 then min(jcwpd172_number) end as jcwpd172_number,
    case when count(distinct jcwpd172_code) = 1 then min(jcwpd172_code) end as jcwpd172_code,
    case when count(distinct point_eu_id_174) = 1 then min(point_eu_id_174) end as point_eu_id_174,
    case when count(distinct jcwpd174_number) = 1 then min(jcwpd174_number) end as jcwpd174_number,
    case when count(distinct jcwpd174_code) = 1 then min(jcwpd174_code) end as jcwpd174_code,
    case when count(distinct puwg1992_x) = 1 then try_cast(replace(min(puwg1992_x), ',', '.') as double) end
        as puwg1992_x,
    case when count(distinct puwg1992_y) = 1 then try_cast(replace(min(puwg1992_y), ',', '.') as double) end
        as puwg1992_y,
    case when count(distinct voivodeship) = 1 then min(voivodeship) end as voivodeship,
    case when count(distinct powiat) = 1 then min(powiat) end as powiat,
    case when count(distinct gmina) = 1 then min(gmina) end as gmina,
    case when count(distinct locality) = 1 then min(locality) end as locality,
    case when count(distinct river_basin) = 1 then min(river_basin) end as river_basin,
    case when count(distinct rzgw) = 1 then min(rzgw) end as rzgw,
    case when count(distinct stratigraphy) = 1 then min(stratigraphy) end as stratigraphy,
    case
        when
            count(distinct aquifer_top_depth_m) = 1
            then try_cast(replace(min(aquifer_top_depth_m), ',', '.') as double)
    end as aquifer_top_depth_m,
    case when count(distinct screened_interval_m) = 1 then min(screened_interval_m) end as screened_interval_m,
    case when count(distinct water_table) = 1 then min(water_table) end as water_table,
    case when count(distinct aquifer_medium) = 1 then min(aquifer_medium) end as aquifer_medium,
    case when count(distinct point_kind) = 1 then min(point_kind) end as point_kind,
    case when count(distinct land_use) = 1 then min(land_use) end as land_use,
    nullif(concat_ws(
        ', ',
        case when count(distinct point_monbada) > 1 then 'point_monbada' end,
        case when count(distinct point_id_monitoring) > 1 then 'point_id_monitoring' end,
        case when count(distinct point_soh) > 1 then 'point_soh' end,
        case when count(distinct point_cbdh) > 1 then 'point_cbdh' end,
        case when count(distinct point_eu_id_161) > 1 then 'point_eu_id_161' end,
        case when count(distinct jcwpd161_number) > 1 then 'jcwpd161_number' end,
        case when count(distinct jcwpd161_code) > 1 then 'jcwpd161_code' end,
        case when count(distinct point_eu_id_172) > 1 then 'point_eu_id_172' end,
        case when count(distinct jcwpd172_number) > 1 then 'jcwpd172_number' end,
        case when count(distinct jcwpd172_code) > 1 then 'jcwpd172_code' end,
        case when count(distinct point_eu_id_174) > 1 then 'point_eu_id_174' end,
        case when count(distinct jcwpd174_number) > 1 then 'jcwpd174_number' end,
        case when count(distinct jcwpd174_code) > 1 then 'jcwpd174_code' end,
        case when count(distinct puwg1992_x) > 1 then 'puwg1992_x' end,
        case when count(distinct puwg1992_y) > 1 then 'puwg1992_y' end,
        case when count(distinct voivodeship) > 1 then 'voivodeship' end,
        case when count(distinct powiat) > 1 then 'powiat' end,
        case when count(distinct gmina) > 1 then 'gmina' end,
        case when count(distinct locality) > 1 then 'locality' end,
        case when count(distinct river_basin) > 1 then 'river_basin' end,
        case when count(distinct rzgw) > 1 then 'rzgw' end,
        case when count(distinct stratigraphy) > 1 then 'stratigraphy' end,
        case when count(distinct aquifer_top_depth_m) > 1 then 'aquifer_top_depth_m' end,
        case when count(distinct screened_interval_m) > 1 then 'screened_interval_m' end,
        case when count(distinct water_table) > 1 then 'water_table' end,
        case when count(distinct aquifer_medium) > 1 then 'aquifer_medium' end,
        case when count(distinct point_kind) > 1 then 'point_kind' end,
        case when count(distinct land_use) > 1 then 'land_use' end
    ), '') as conflicting_attributes,
    count(distinct voivodeship) as voivodeship_values,
    count(*) as samples,
    string_agg(distinct split_part(entity, '_', 3), ',' order by split_part(entity, '_', 3)) as monitoring_kinds,
    string_agg(distinct raw_sha256, ',' order by raw_sha256) as raw_sha256,
    string_agg(distinct source_version, ',' order by source_version) as source_version
from samples
group by point_key, survey_year
