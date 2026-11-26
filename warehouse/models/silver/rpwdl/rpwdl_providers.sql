{# RPWDL healthcare providers (podmioty wykonujące działalność leczniczą), register PLS-020: one row per provider
   ledger (księga rejestrowa) of the newest active and inactive register files (bronze.rpwdl_providers). Grain: the
   file row (entity x source_row_number): a provider can be one natural person, so no identifier is loaded (no
   ledger id, book number, REGON, NIP, name or address; aggregate counts only). Seat: voivodeship only (Bronze
   keeps the first two TERYT digits). ledger_state = the file the row came from; is_active = an active ledger, not
   removed and not ended on the snapshot date (the file's download day). Codes as published: authority_code = the
   registering voivode (TERC voivodeship code), legal_form_code, founder_code (founding body). #}
with newest as ({{ poc_rpwdl_newest('rpwdl_providers') }}
),

typed as (
    select
        entity,
        source_row_number,
        case when ends_with(entity, '_active') then 'active' else 'inactive' end as ledger_state,
        {{ poc_rpwdl_text('authority_type') }} as authority_type,
        {{ poc_rpwdl_text('authority_code') }} as authority_code,
        {{ poc_rpwdl_text('legal_form_code') }} as legal_form_code,
        {{ poc_rpwdl_text('legal_form_name') }} as legal_form_name,
        {{ poc_rpwdl_text('founder_code') }} as founder_code,
        {{ poc_rpwdl_voivodeship('voivodeship_teryt') }} as voivodeship_code,
        {{ poc_rpwdl_date('start_date', 'rpwdl_providers.start_date') }} as start_date,
        {{ poc_rpwdl_date('end_date', 'rpwdl_providers.end_date') }} as end_date,
        {{ poc_rpwdl_date('removal_date', 'rpwdl_providers.removal_date') }} as removal_date,
        {{ poc_rpwdl_date('entry_change_date', 'rpwdl_providers.entry_change_date') }} as entry_change_date,
        {{ poc_rpwdl_date('suspension_start', 'rpwdl_providers.suspension_start') }} as suspension_start,
        {{ poc_rpwdl_date('suspension_end', 'rpwdl_providers.suspension_end') }} as suspension_end,
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
