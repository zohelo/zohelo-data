{{ config(alias='build_mf_biala_lista', tags=['gold_block']) }}
{# Gold build stage (#2740): MF VAT white list flat file, an entity snapshot of the newest daily file
   (silver.biala_lista_taxpayer_hashes and silver.biala_lista_masks, latest-only: one date, the file's
   generation date). The flat file publishes only SHA-512 hashes of (date + NIP + bank account), so the
   counts are entries (one per taxpayer × account), not taxpayers, and carry no region: geography PL.
   Measures: mf_biala_lista:flat_file:entries with breakdown taxpayer_status (active / exempt VAT
   taxpayers, _T both) and mf_biala_lista:flat_file:masks (virtual-account masks). #}
with by_status as (
    select
        file_date,
        taxpayer_status,
        count(*) as n,
        max(source_version) as source_version,
        any_value(raw_sha256) as raw_sha256
    from {{ source('silver_entities', 'biala_lista_taxpayer_hashes') }}
    group by file_date, taxpayer_status
),

entries as (
    select * from by_status
    union all
    select
        file_date,
        '_T' as taxpayer_status,
        sum(n) as n,
        max(source_version) as source_version,
        any_value(raw_sha256) as raw_sha256
    from by_status
    group by file_date
),

masks as (
    select
        file_date,
        count(*) as n,
        max(source_version) as source_version,
        any_value(raw_sha256) as raw_sha256
    from {{ source('silver_entities', 'biala_lista_masks') }}
    group by file_date
)

select
    'mf_biala_lista' as source_system,
    'flat_file' as block_id,
    'MF VAT white list, flat file' as block_name,
    'mf_biala_lista:flat_file:entries' as measure_key,
    'VAT white list entries (taxpayer × bank account hashes)' as measure_name,
    'entries' as measure_codes,
    'list_entries' as unit_code,
    'list entries' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'D' as frequency,
    file_date as date_key,
    cast(file_date as varchar) as period_label,
    cast(json_object('taxpayer_status', taxpayer_status) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    taxpayer_status = '_T' as is_total,
    cast(n as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from entries
union all
select
    'mf_biala_lista' as source_system,
    'flat_file' as block_id,
    'MF VAT white list, flat file' as block_name,
    'mf_biala_lista:flat_file:masks' as measure_key,
    'VAT white list virtual-account masks' as measure_name,
    'masks' as measure_codes,
    'list_entries' as unit_code,
    'list entries' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'D' as frequency,
    file_date as date_key,
    cast(file_date as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(n as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from masks
