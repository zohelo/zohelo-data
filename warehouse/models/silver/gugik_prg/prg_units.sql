{# PRG boundary features of the newest national package, typed: one row per unit of each
   layer (country, voivodeships, powiats, gminas, cadastral units and precincts), keyed by
   the TERYT code as published (JPT_KOD_JE), with the area in hectares (JPT_POWIER) and
   the geometry as WKB in the published CRS. #}
with newest as (
    select * from {{ source('gugik_prg', 'prg_boundaries') }}
    where
        source_version = (select max(latest.source_version) from {{ source('gugik_prg', 'prg_boundaries') }} as latest)
)

select
    layer,
    case
        when layer like 'A00%' then 'kraj'
        when layer like 'A01%' then 'wojewodztwo'
        when layer like 'A02%' then 'powiat'
        when layer like 'A03%' then 'gmina'
        when layer like 'A05%' then 'jednostka_ewidencyjna'
        when layer like 'A06%' then 'obreb_ewidencyjny'
    end as unit_level,
    jpt_kod_je as teryt_code,
    jpt_nazwa_ as name_pl,
    try_cast(jpt_powier as double) as area_ha,
    try_cast(substr(cast(wazny_od as varchar), 1, 10) as date) as valid_from,
    try_cast(substr(cast(wazny_do as varchar), 1, 10) as date) as valid_to,
    regon,
    geometry_type,
    geometry_wkb,
    raw_sha256,
    source_version
from newest
