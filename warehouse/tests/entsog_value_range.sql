-- entsog (gold.build_entsog): values lie within a plausible range for their unit, to catch a unit or scale error
-- (Wh read as kWh, a sum over the wrong grain), not small mistakes. Flows are not negative (direction is the sign).
-- Bounds from the Dev trial on the final head (docs/sources/entsog.md).
select
    measure_key,
    breakdown_json,
    date_key,
    value
from {{ ref('poc_build_entsog') }}
where value < 0
    or (frequency = 'D' and value > 5e9)
    or (frequency = 'M' and value > 1.5e11)
