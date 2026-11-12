-- Gold metrics (decision D33): a KPI measure whose source unit (unit_map.csv row of its unit code) and target
-- unit (kpi_measure.csv unit_key, else the KPI's unit) have different base units in unit.csv. The metrics view
-- converts only pure scale (thousand persons → persons, ha → km², usd → mln_usd); a different base means a
-- wrong mapping or a target in another currency, index base or quantity, which is never converted. Fix the
-- unit_map row or the kpi_measure unit_key, never the value.
select
    concept_key,
    measure_key,
    source_system,
    unit_code,
    source_unit_key,
    source_base_unit_key,
    target_unit_key,
    target_base_unit_key
from ({{ poc_measure_units() }})
where
    source_unit_key is not null
    and source_base_unit_key is distinct from target_base_unit_key
