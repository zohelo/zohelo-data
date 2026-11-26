-- ARiMR direct-payment statistics (gold.build_arimr_payments): every value lies within a plausible range for its
-- measure; Poland is the largest area. Report of 30.09.2026 (probe 43345651745): amount paid, highest campaign 2022 =
-- 16,858,050,328 PLN; applications 2021 = 1,282,582; decisions 2021 = 1,262,580 (2004 campaign about 1.4 million).
-- The bounds catch scale and column errors (a value x1000, an amount read as a count), not small mistakes. A live
-- test on the build stage, as no metrics model reads this source yet. Returns the rows outside.
select
    measure_key,
    geography_key,
    date_key,
    value
from {{ ref('poc_build_arimr_payments') }}
where
    value < 0
    or (measure_key like '%:amount_paid' and value > 3e10)
    or (measure_key not like '%:amount_paid' and value > 3e6)
