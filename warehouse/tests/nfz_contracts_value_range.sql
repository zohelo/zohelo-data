-- NFZ contracts (gold.build_nfz_contracts): every value lies within a plausible range for its unit, for Poland, the
-- NFZ branches and each service type, and Poland's total contracted amount of every contract year but the newest
-- (a year NFZ has just opened can still be small) is at least 10 bn PLN. The bounds catch a unit or scale error (an
-- amount in grosze or in thousands: Poland's total would be about 1.7e8 instead of 1.7e11; a count summed twice),
-- not small mistakes. Measured values: docs/sources/nfz_contracts.md ("Trial"). A live test on the build stage: no
-- metrics model of this source is in tests/metrics_value_range.sql's union. Returns the rows outside.
with build as (
    select * from {{ ref('poc_build_nfz_contracts') }}
),

newest as (
    select max(date_key) as date_key from build
)

select
    b.measure_key,
    b.geography_key,
    b.breakdown_json,
    b.date_key,
    b.value
from build as b
cross join newest as n
where
    (b.unit_code = 'agreements' and (b.value < 1 or b.value > 100000))
    or (b.unit_code = 'healthcare_providers' and (b.value < 1 or b.value > 60000))
    or (b.unit_code = 'pln' and (b.value < 0 or b.value > 5e11))
    or (
        b.unit_code = 'pln' and b.is_total and b.geography_key = 'PL' and b.date_key < n.date_key
        and b.value < 1e10
    )
    or b.unit_code not in ('agreements', 'healthcare_providers', 'pln')
