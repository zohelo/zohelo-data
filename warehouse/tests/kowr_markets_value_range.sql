-- KOWR FOB export prices (gold.build_kowr_markets): every price lies between 50 and 1000 EUR/t. The 1,090 quotations
-- of March 2023 to November 2026 range from 154 to 358 EUR/t (HRW US Gulf, 29.03.2023; Dev trial 45920665418); the
-- bounds catch a zł/t value or a change in % read as the price, not small mistakes. A live test on the build stage,
-- as no metrics model reads this source yet. Returns the rows outside.
select
    measure_key,
    date_key,
    value
from {{ ref('poc_build_kowr_markets') }}
where value < 50 or value > 1000
