-- KOWR EU purchase prices (silver.kowr_markets_eu_prices): every period has the three commodities with the 27
-- member states and the EU average each (84 rows, a price or a flag). Returns the periods with another shape.
select
    period_start,
    count(distinct commodity_code) as commodities,
    count(*) as n
from {{ ref('kowr_markets_eu_prices') }}
group by period_start
having count(distinct commodity_code) <> 3 or count(*) <> 84
