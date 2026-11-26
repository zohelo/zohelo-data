-- MRiRW ZSRIR: every market bulletin in Bronze (entity = cereals, milk, pork) yields Silver rows with a price. A
-- monthly sheet or grid header the Silver model no longer finds would otherwise drop that market silently; returns
-- the entities without a priced Silver row.
select distinct b.entity
from {{ source('mrirw_zsrir', 'mrirw_zsrir_cells') }} as b
left join (
    select distinct market
    from {{ ref('mrirw_zsrir_prices') }}
    where value is not null
) as s on b.entity = s.market
where s.market is null
