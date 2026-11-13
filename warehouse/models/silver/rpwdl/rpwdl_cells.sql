{# RPWDL organisational cells (komórki organizacyjne: wards, clinics, labs) of the healthcare providers' units,
   register PLS-020: one row per cell (cell_id) of the newest active and inactive provider files
   (bronze.rpwdl_cells). Grain: cell_id. Allowlist: no name, address or contact columns. specialty_code =
   department code part VIII as published, specialty_name its label. Bed and place counts as published (a missing
   count stays null; text that is not a count fails the build). is_active = in an active ledger file and not
   ended on the snapshot date (a suspended cell counts). #}
with newest as ({{ poc_rpwdl_newest('rpwdl_cells') }}
),

typed as (
    select
        cell_id,
        unit_id,
        establishment_id,
        book_id,
        {{ poc_rpwdl_text('regon') }} as regon,
        case when ends_with(entity, '_active') then 'active' else 'inactive' end as ledger_state,
        {{ poc_rpwdl_text('unit_code') }} as unit_code,
        {{ poc_rpwdl_text('cell_code') }} as cell_code,
        {{ poc_rpwdl_text('specialty_code') }} as specialty_code,
        {{ poc_rpwdl_text('specialty_name') }} as specialty_name,
        {{ poc_rpwdl_text('teryt') }} as teryt,
        {{ poc_rpwdl_voivodeship('teryt') }} as voivodeship_code,
        {{ poc_rpwdl_count('beds_total', 'rpwdl_cells.beds_total') }} as beds_total,
        {{ poc_rpwdl_count('beds_intensive_care', 'rpwdl_cells.beds_intensive_care') }} as beds_intensive_care,
        {{ poc_rpwdl_count('dialysis_stations', 'rpwdl_cells.dialysis_stations') }} as dialysis_stations,
        {{ poc_rpwdl_count('day_places', 'rpwdl_cells.day_places') }} as day_places,
        {{ poc_rpwdl_date('start_date', 'rpwdl_cells.start_date') }} as start_date,
        {{ poc_rpwdl_date('end_date', 'rpwdl_cells.end_date') }} as end_date,
        {{ poc_rpwdl_date('suspension_start', 'rpwdl_cells.suspension_start') }} as suspension_start,
        {{ poc_rpwdl_date('suspension_end', 'rpwdl_cells.suspension_end') }} as suspension_end,
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
