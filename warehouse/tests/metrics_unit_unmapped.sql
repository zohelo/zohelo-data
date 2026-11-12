-- Gold metrics (decision D33): KPI measures whose source unit code has no unit_map.csv row (or whose target
-- unit is not in unit.csv). Their values stay as published, without conversion, and the metrics `unit` is the published unit code.
-- A warning, not a failure: add the unit_map row (with a measure_key_pattern when the code's meaning depends
-- on the measure) once the source's own unit is confirmed.
{{ config(severity='warn') }}

select
    concept_key,
    measure_key,
    source_system,
    unit_code,
    target_unit_key
from ({{ poc_measure_units() }})
where source_unit_key is null or target_base_unit_key is null
