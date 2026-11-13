-- Gold build stage of the NBP gold price, by rule (docs/gold-coverage.md): every fixing of
-- silver.nbp_gold_fixings in the common observation shape (the 21 columns of poc_build_nbp.sql, same
-- order). Applied by warehouse/observations.py to the table's files as one block. Relation `silver`: the
-- data files of silver.nbp_gold_fixings (one row per effective date).
--   measure_key = nbp:nbp_gold_fixings:PLN/g:price@D; no breakdown; no territory (_Z)
--   value       = PLN per gram of gold of 1000 fineness, as published; a date without a price is no row
select
    'nbp' as source_system,
    'nbp_gold_fixings' as block_id,
    'NBP gold fixings' as block_name,
    'nbp:nbp_gold_fixings:PLN/g:price@D' as measure_key,
    'Gold price, NBP fixing (PLN per gram, 1000 fineness)' as measure_name,
    'price' as measure_codes,
    'PLN/g' as unit_code,
    'PLN per gram' as unit_name,
    cast(null as varchar) as geography_source_code,
    '_Z' as geography_key,
    'D' as frequency,
    s.effective_date as date_key,
    cast(s.effective_date as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(s.price_pln_per_gram as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from silver as s
where s.price_pln_per_gram is not null and s.effective_date is not null
