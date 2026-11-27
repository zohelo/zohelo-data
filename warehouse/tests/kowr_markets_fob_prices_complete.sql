-- KOWR FOB export prices (silver.kowr_markets_fob_prices): every quotation date carries the three commodity groups
-- (milling wheat, feed barley, maize), with or without a price, as Bronze accepts only reports that print all three.
-- A group the Silver model stops reading leaves a date short and fails here. Returns the dates with another count.
select
    quote_date,
    count(distinct commodity_code) as commodities
from {{ ref('kowr_markets_fob_prices') }}
group by quote_date
having count(distinct commodity_code) <> 3
