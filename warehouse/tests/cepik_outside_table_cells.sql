{{ config(severity='warn') }}
-- cepik: numbers printed to the right of a block's month columns (cell_status 'outside_table'), which belong to no
-- month and are kept out of Gold. A warning, not an error: the producer's workbook of 26 Nov 2026 repeats September
-- 2026 in an unheaded column; each run lists such cells so a change of layout is seen.
select
    entity,
    group_code,
    value
from {{ ref('cepik_vehicle_registrations') }}
where cell_status = 'outside_table'
