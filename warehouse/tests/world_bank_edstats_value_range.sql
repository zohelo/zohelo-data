-- OECD PISA mean scores (World Bank EdStats republication, gold.build_world_bank_edstats): every value lies within
-- the plausible range of the PISA scale for a country mean. Measured 257.4 to 615.6 over all economies and cycles
-- 2000-2018 (Dev trial 43291944406); the bounds catch scale or unit errors, not small mistakes. A live test on the
-- build stage, as no PISA metrics model is in tests/metrics_value_range.sql's union. Returns the rows outside.
select
    measure_key,
    geography_key,
    date_key,
    breakdown_json,
    value
from {{ ref('poc_build_world_bank_edstats') }}
where value < 250 or value > 650
