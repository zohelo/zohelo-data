{# RPWDL establishments (zakłady lecznicze) of the healthcare providers, register PLS-020: one row per establishment
   (establishment_id, "ID ZOZ") of the newest active and inactive provider files (bronze.rpwdl_establishments). Grain:
   snapshot_year x establishment_id. Allowlist: no provider ledger id, REGON, name, address or contact columns (the id
   kept identifies a place of care, not its provider). activity_kind as published (1, 2, 3 or an HP code; 'brak wpisu'
   and NULL = null). is_active = in an active ledger file and not ended on the snapshot date.
   Years (#3139): the newest files of each snapshot year (snapshot_year = the year of the entity), so a past year keeps
   its last state; snapshot_date = the download day of the file. #}
with newest as ({{ poc_rpwdl_newest('rpwdl_establishments') }}
),

typed as (
    select
        establishment_id,
        {{ poc_rpwdl_ledger_state() }} as ledger_state,
        {{ poc_rpwdl_text('activity_kind') }} as activity_kind,
        {{ poc_rpwdl_text('teryt') }} as teryt,
        {{ poc_rpwdl_voivodeship('teryt') }} as voivodeship_code,
        {{ poc_rpwdl_date('start_date', 'rpwdl_establishments.start_date') }} as start_date,
        {{ poc_rpwdl_date('end_date', 'rpwdl_establishments.end_date') }} as end_date,
        {{ poc_rpwdl_year() }} as snapshot_year,
        {{ poc_rpwdl_snapshot() }} as snapshot_date,
        raw_sha256,
        source_version,
        ingested_at
    from newest
)

select
    *,
    ledger_state = 'active' and coalesce(end_date > snapshot_date, true) as is_active
from typed
