{# PRG address points per gmina (PL-ENV-007, #2870): the gminas' address registers (EMUiA) as integrated in the
   state register of boundaries, from the newest national package (bronze.prg_address_points, one row per address
   point, attributes as text). Grain: one row per gmina TERC code (TERYT_GMI, 7 digits WWPPGGT as published).
   The ~8.6 M points stay in Bronze: a full-rebuild model stays below 1 M rows (#822, D28), and the package has
   no changed-since to load points incrementally; Silver holds what Gold needs, typed counts per gmina. A point
   without a locality (ID_SIMC), street (ID_ULIC) or number is counted, never dropped; no value is invented. #}
with points as (
    select
        nullif(trim(teryt_gmi), '') as terc_code,
        nullif(trim(id_simc), '') as simc_code,
        nullif(trim(id_ulic), '') as ulic_code,
        nullif(trim(numer_porz), '') as house_number,
        source_version,
        raw_sha256
    from {{ source('gugik_prg', 'prg_address_points') }}
    where
        ingested_at = (select max(newest.ingested_at) from {{ source('gugik_prg', 'prg_address_points') }} as newest)
)

select
    terc_code,
    substr(terc_code, 1, 2) as woj_code,
    substr(terc_code, 1, 4) as pow_code,
    substr(terc_code, 7, 1) as gmina_type_code,
    count(*) as address_points,
    count(ulic_code) as points_with_street,
    count(*) - count(simc_code) as points_without_locality,
    count(*) - count(house_number) as points_without_number,
    count(distinct simc_code) as localities,
    source_version,
    raw_sha256
from points
group by terc_code, source_version, raw_sha256
