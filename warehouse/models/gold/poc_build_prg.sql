{{ config(alias='build_prg', tags=['gold_block'], meta={'optional_parents': ['poc_prg_address_counts']}) }}
{# Gold build stage of GUGiK PRG, two blocks.
   'jednostki_administracyjne' (#2739): the area of every Polish administrative unit as the state register of
   boundaries (silver.prg_units) publishes it (JPT_POWIER, in hectares, stored as published: decision D33; KPI area
   converts to km² in metrics_area), one daily observation at the date of the PRG edition held (latest-only policy:
   one date). Voivodeships, powiats and the 2,479 gminas with a boundary (TERC keys); not the country unit, whose
   polygon includes internal and territorial sea waters (322,720 km² against 313,937 km² of land units). Third source
   of KPI area beside GUS BDL P1410 (which takes its areas from PRG on 1 January) and the World Bank.
   'punkty_adresowe' (#3106, register PL-ENV-007): the number of address points of the newest national address
   package per gmina, powiat, voivodeship and Poland (models/gold/poc_prg_address_counts.sql, read through
   poc_optional_ref: zero rows until silver.prg_address_points exists, so the boundaries block never waits), an
   entity-snapshot count at the package's Last-Modified date; codes matched through map_geography, a code PRG's
   boundaries do not know maps to _X with its code kept. #}
with addresses as (
    select * from {{ poc_optional_ref('poc_prg_address_counts',
        'unit_level:varchar, area_code:varchar, address_points:bigint, source_version:varchar, raw_sha256:varchar') }}
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'gugik_prg'
)

select
    'gugik_prg' as source_system,
    'jednostki_administracyjne' as block_id,
    'PRG administrative units' as block_name,
    'gugik_prg:jednostki_administracyjne:ha:area' as measure_key,
    'Area of the administrative unit (PRG, JPT_POWIER)' as measure_name,
    'area' as measure_codes,
    'ha' as unit_code,
    'hectares' as unit_name,
    teryt_code as geography_source_code,
    'TERC:' || teryt_code as geography_key,
    'D' as frequency,
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as date_key,
    cast(cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    area_ha as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from {{ ref('prg_units') }}
where unit_level in ('wojewodztwo', 'powiat', 'gmina') and area_ha is not null
union all
select
    'gugik_prg' as source_system,
    'punkty_adresowe' as block_id,
    'PRG address points' as block_name,
    'gugik_prg:punkty_adresowe:address_points' as measure_key,
    'Address points in the state register of boundaries (PRG)' as measure_name,
    'address_points' as measure_codes,
    'address_points' as unit_code,
    'address points' as unit_name,
    a.area_code as geography_source_code,
    case when a.unit_level = 'kraj' then 'PL' else coalesce(g.geography_key, '_X') end as geography_key,
    'D' as frequency,
    cast(strptime(a.source_version, '%Y%m%dT%H%M%SZ') as date) as date_key,
    cast(cast(strptime(a.source_version, '%Y%m%dT%H%M%SZ') as date) as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(a.address_points as double) as value,
    'ok' as observation_status_key,
    a.source_version,
    a.raw_sha256
from addresses as a
left join geo_map as g on a.unit_level <> 'kraj' and a.area_code = g.source_code
