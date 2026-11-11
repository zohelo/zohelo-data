{{ config(alias='build_prg') }}
{# Gold build stage (#2739): the area of every Polish administrative unit as the state register of
   boundaries (GUGiK PRG, silver.prg_units) publishes it (JPT_POWIER, hectares, here km²), one daily
   observation at the date of the PRG edition held (latest-only policy: one date). Country, voivodeships,
   powiats and the 2,479 gminas with a boundary; geography keys TERC / PL. Second source of KPI area
   beside GUS BDL P1410 (which takes its areas from PRG on 1 January). #}
select
    'gugik_prg' as source_system,
    'jednostki_administracyjne' as block_id,
    'PRG administrative units' as block_name,
    'gugik_prg:jednostki_administracyjne:km2:area' as measure_key,
    'Area of the administrative unit (PRG, JPT_POWIER)' as measure_name,
    'area' as measure_codes,
    'km2' as unit_code,
    'square kilometres' as unit_name,
    teryt_code as geography_source_code,
    case when unit_level = 'kraj' then 'PL' else 'TERC:' || teryt_code end as geography_key,
    'D' as frequency,
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as date_key,
    cast(cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    area_ha / 100 as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from {{ ref('prg_units') }}
where unit_level in ('kraj', 'wojewodztwo', 'powiat', 'gmina') and area_ha is not null
