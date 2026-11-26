-- KOWR FOB export prices (gold.build_kowr_markets): every price lies between 50 and 1000 EUR/t. Probe reports of
-- 2023 to 2026 (Dev probe 45895465874) range from 167 (Black Sea feed barley, 02.07.2025) to 358 EUR/t (HRW US Gulf,
-- 29.03.2023); the bounds catch a zł/t value or a change in % read as the price, not small mistakes. A live test on
-- the build stage, as no metrics model reads this source yet. Returns the rows outside.
select
    measure_key,
    date_key,
    value
from {{ ref('poc_build_kowr_markets') }}
where value < 50 or value > 1000
