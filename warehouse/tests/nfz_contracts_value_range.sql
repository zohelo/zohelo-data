-- NFZ contracts (gold.build_nfz_contracts): every value lies within a plausible range for its unit, for Poland, the
-- NFZ branches and each service type. The bounds catch a unit or scale error (an amount in grosze or thousands, a
-- count summed twice), not small mistakes. Measured on Dev trial 43328769016 (contract years 2019-2026): agreements
-- 1 to 45,846 (Poland 2025), providers 1 to 26,538 (Poland 2026), contracted amount 0 to 174,340,312,472.51 PLN
-- (Poland 2026; published amounts of 0 exist). A live test on the build stage: no metrics model of this source is in
-- tests/metrics_value_range.sql's union. Returns the rows outside.
select
    measure_key,
    geography_key,
    date_key,
    value
from {{ ref('poc_build_nfz_contracts') }}
where
    (unit_code = 'agreements' and (value < 1 or value > 100000))
    or (unit_code = 'healthcare_providers' and (value < 1 or value > 60000))
    or (unit_code = 'pln' and (value < 0 or value > 5e11))
    or unit_code not in ('agreements', 'healthcare_providers', 'pln')
