{# RPWDL professional practices (praktyki zawodowe: physicians, nurses and midwives, physiotherapists, laboratory
   diagnosticians; individual and group), register PLS-020: one row per practice entry of the newest active and
   inactive file of each of the 8 practice registers (bronze.rpwdl_practices). Grain: the file row (entity x
   source_row_number): an individual practice is one practitioner, so no identifier is loaded (no register id, book
   number, name, business-register number or address; aggregate counts only). Only the voivodeship of the
   correspondence address is kept (Bronze keeps the first two TERYT digits). Chamber and legal form codes as
   published. is_active = in an active ledger file, not removed and not ended on the snapshot date. #}
with newest as ({{ poc_rpwdl_newest('rpwdl_practices') }}
),

typed as (
    select
        entity,
        source_row_number,
        regexp_replace(entity, '_(active|inactive)$', '') as practice_register,
        case when ends_with(entity, '_active') then 'active' else 'inactive' end as ledger_state,
        {{ poc_rpwdl_text('practice_kind') }} as practice_kind,
        {{ poc_rpwdl_text('chamber_code') }} as chamber_code,
        {{ poc_rpwdl_text('chamber_name') }} as chamber_name,
        {{ poc_rpwdl_text('legal_form_code') }} as legal_form_code,
        {{ poc_rpwdl_text('legal_form_name') }} as legal_form_name,
        {{ poc_rpwdl_voivodeship('voivodeship_teryt') }} as voivodeship_code,
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
