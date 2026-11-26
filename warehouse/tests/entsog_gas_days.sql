-- entsog (silver.entsog_physical_flow): every record is one gas day. periodFrom starts at 03:00, 04:00 or 05:00 WET
-- (Dev probe 43320099570: 05:00 at most points, 03:00 or 04:00 at some), and periodTo is on the next date, so
-- gas_day = the date of periodFrom. A record of another length, or a record with a value and another start, fails
-- here instead of shifting a day. A record without a value (isNA) only needs the length: GAZ-SYSTEM publishes the
-- Compulsory Stocks Exit Point (ITP-00543, never a value) with some isNA days starting at 07:00 (89 records,
-- Jan-Feb 2023 and Nov 2026; Dev run 45713837105, #3196); they add nothing to a sum.
select
    point_key,
    direction_key,
    period_from,
    period_to
from {{ ref('entsog_physical_flow') }}
where (value_kwh_d is not null and substr(period_from, 12, 2) not in ('03', '04', '05'))
    or cast(left(period_to, 10) as date) <> gas_day + 1
