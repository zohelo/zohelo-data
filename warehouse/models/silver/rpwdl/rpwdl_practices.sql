{# RPWDL professional practices (praktyki zawodowe: physicians, nurses and midwives, physiotherapists, laboratory
   diagnosticians; individual and group), register PLS-020: one row per practice entry of the newest active and
   inactive file of each of the 8 practice registers (bronze.rpwdl_practices). Grain: practice_register x
   practice_id (each professional chamber keeps its own register). Privacy allowlist: no name, no business-register
   number, no address; only the voivodeship of the correspondence address is kept (from its TERYT code). Chamber
   and legal form codes as published. is_active = in an active ledger file, not removed and not ended on the
   snapshot date. #}
with newest as ({{ poc_rpwdl_newest('rpwdl_practices') }}
),

typed as (
    select
        regexp_replace(entity, '_(active|inactive)$', '') as practice_register,
        practice_id,
        {{ poc_rpwdl_text('book_number') }} as book_number,
        case when ends_with(entity, '_active') then 'active' else 'inactive' end as ledger_state,
        {{ poc_rpwdl_text('practice_kind') }} as practice_kind,
        {{ poc_rpwdl_text('chamber_code') }} as chamber_code,
        {{ poc_rpwdl_text('chamber_name') }} as chamber_name,
        {{ poc_rpwdl_text('legal_form_code') }} as legal_form_code,
        {{ poc_rpwdl_text('legal_form_name') }} as legal_form_name,
        {{ poc_rpwdl_voivodeship('teryt') }} as voivodeship_code,
        {{ poc_rpwdl_date('start_date', 'rpwdl_practices.start_date') }} as start_date,
        {{ poc_rpwdl_date('end_date', 'rpwdl_practices.end_date') }} as end_date,
        {{ poc_rpwdl_date('removal_date', 'rpwdl_practices.removal_date') }} as removal_date,
        {{ poc_rpwdl_date('entry_change_date', 'rpwdl_practices.entry_change_date') }} as entry_change_date,
        {{ poc_rpwdl_snapshot() }} as snapshot_date,
        raw_sha256,
        source_version,
        ingested_at
    from newest
)

select
    *,
    ledger_state = 'active' and removal_date is null and coalesce(end_date > snapshot_date, true) as is_active
from typed
