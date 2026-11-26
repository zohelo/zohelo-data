{{ config(alias='prg_address_counts') }}
{# Gold (#3106, register PL-ENV-007): the number of address points of the newest PRG national address package
   (silver.prg_address_points, one row per address point, latest-only: one package), per area: each gmina by the
   point's own 7-digit TERC code (gmina_code), each powiat (its first 4 digits), each voivodeship (first 2 digits)
   and Poland ('PL'). A point without a well-formed gmina code counts for Poland only, so the gminas do not add up to
   Poland when such points exist (the trial counts them). The block 'punkty_adresowe' of the build stage
   poc_build_prg reads this model through poc_optional_ref, so the boundaries block never waits for the address
   table's first load. Snapshot date = the package's Last-Modified (source_version). #}
with points as (
    select
        case when regexp_full_match(coalesce(gmina_code, ''), '[0-9]{7}') then gmina_code end as gmina_code,
        source_version,
        raw_sha256
    from {{ source('gugik_prg_silver', 'prg_address_points') }}
),

located as (
    select
        'kraj' as unit_level,
        'PL' as area_code,
        source_version,
        raw_sha256
    from points
    union all
    select
        'wojewodztwo' as unit_level,
        left(gmina_code, 2) as area_code,
        source_version,
        raw_sha256
    from points
    where gmina_code is not null
    union all
    select
        'powiat' as unit_level,
        left(gmina_code, 4) as area_code,
        source_version,
        raw_sha256
    from points
    where gmina_code is not null
    union all
    select
        'gmina' as unit_level,
        gmina_code as area_code,
        source_version,
        raw_sha256
    from points
    where gmina_code is not null
)

select
    unit_level,
    area_code,
    count(*) as address_points,
    max(source_version) as source_version,
    min(raw_sha256) as raw_sha256,
    count(distinct raw_sha256) as packages
from located
group by unit_level, area_code
