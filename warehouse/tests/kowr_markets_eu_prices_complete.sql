-- KOWR EU purchase prices (silver.kowr_markets_eu_prices): every report week has the three commodities with the 27
-- member states and the EU average each (84 rows, a price or a flag). Returns the weeks with another shape.
select
    report_week,
    count(distinct commodity_code) as commodities,
    count(*) as n
from {{ ref('kowr_markets_eu_prices') }}
group by report_week
having count(distinct commodity_code) <> 3 or count(*) <> 84
