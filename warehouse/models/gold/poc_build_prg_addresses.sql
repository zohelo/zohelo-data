{{ config(alias='build_prg_addresses', tags=['gold_block']) }}
{# Gold build stage (PL-ENV-007, #2870): the number of address points per gmina in the newest national PRG
   address package (silver.prg_address_gminas), an entity-snapshot count like build_gleif: one daily observation
   per gmina at the package's Last-Modified date (latest-only: one date, no history). Geography by the gmina's
   TERC code through map_geography (source_system gugik_prg: the PRG unit codes, joined by code, never by name);
   a code that matches no PRG gmina stays `_X` with its code and is counted in build_check
   (rows_geography_unmatched). Gminas only: no powiat, voivodeship or country rows are derived here. #}
with gminas as (
    select * from {{ ref('prg_address_gminas') }}
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'gugik_prg'
)

select
    'gugik_prg' as source_system,
    'punkty_adresowe' as block_id,
    'PRG address points' as block_name,
    'gugik_prg:punkty_adresowe:address_points' as measure_key,
    'Address points in the gmina (PRG)' as measure_name,
    'address_points' as measure_codes,
    'address_points' as unit_code,
    'address points' as unit_name,
    a.terc_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
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
from gminas as a
left join geo_map as g on a.terc_code = g.source_code
