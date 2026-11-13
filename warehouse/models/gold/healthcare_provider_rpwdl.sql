{# Gold entity table (register PLS-020, #2958): one row per RPWDL healthcare provider ledger (book_id), newest
   register files (silver.rpwdl_providers). Keys: book_id (register), regon (official business identifier, as
   published); areas as dim_geography keys: geography_key = 'TERC:' || the 7-digit TERYT code of the seat (gmina
   level, null when the file gives fewer digits), voivodeship_geography_key = 'TERC:' || code || '00000'. Privacy
   allowlist: no names, NIP or addresses (load.py). #}
select
    book_id,
    book_number,
    regon,
    legal_form_code,
    legal_form_name,
    founder_code,
    founder_name,
    authority_code,
    case when regexp_full_match(teryt, '[0-9]{7}') and voivodeship_code is not null then 'TERC:' || teryt end
        as geography_key,
    'TERC:' || voivodeship_code || '00000' as voivodeship_geography_key,
    start_date,
    end_date,
    removal_date,
    ledger_state,
    is_active,
    snapshot_date,
    source_version,
    raw_sha256
from {{ ref('rpwdl_providers') }}
