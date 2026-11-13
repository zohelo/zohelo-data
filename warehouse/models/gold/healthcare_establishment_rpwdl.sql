{# Gold entity table (register PLS-020, #2958): one row per RPWDL establishment (zakład leczniczy,
   establishment_id) with its provider ledger (book_id), newest register files (silver.rpwdl_establishments).
   Areas as in healthcare_provider_rpwdl (establishment address TERYT). No names or addresses. #}
select
    establishment_id,
    book_id,
    regon,
    activity_kind,
    case when regexp_full_match(teryt, '[0-9]{7}') and voivodeship_code is not null then 'TERC:' || teryt end
        as geography_key,
    'TERC:' || voivodeship_code || '00000' as voivodeship_geography_key,
    start_date,
    end_date,
    ledger_state,
    is_active,
    snapshot_date,
    source_version,
    raw_sha256
from {{ ref('rpwdl_establishments') }}
