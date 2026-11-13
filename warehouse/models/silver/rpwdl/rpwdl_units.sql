{# RPWDL organisational units (jednostki organizacyjne) of the healthcare providers' establishments, register
   PLS-020: one row per unit (unit_id) of the newest active and inactive provider files (bronze.rpwdl_units).
   Grain: unit_id. Allowlist: no name, address or contact columns. unit_code = the department code part V as
   published. is_active = in an active ledger file and not ended on the snapshot date (a suspended unit counts). #}
with newest as ({{ poc_rpwdl_newest('rpwdl_units') }}
),

typed as (
    select
        unit_id,
        establishment_id,
        book_id,
        {{ poc_rpwdl_text('regon') }} as regon,
        case when ends_with(entity, '_active') then 'active' else 'inactive' end as ledger_state,
        {{ poc_rpwdl_text('unit_code') }} as unit_code,
        {{ poc_rpwdl_text('teryt') }} as teryt,
        {{ poc_rpwdl_voivodeship('teryt') }} as voivodeship_code,
        {{ poc_rpwdl_date('start_date', 'rpwdl_units.start_date') }} as start_date,
        {{ poc_rpwdl_date('end_date', 'rpwdl_units.end_date') }} as end_date,
        {{ poc_rpwdl_date('suspension_start', 'rpwdl_units.suspension_start') }} as suspension_start,
        {{ poc_rpwdl_date('suspension_end', 'rpwdl_units.suspension_end') }} as suspension_end,
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
