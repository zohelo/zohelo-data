-- UNCTAD Gold build: every economy code of poc_unctad_areas (EU27 members, 3412 European Union (2020 …), 0000 World)
-- is present in each of the three tables gold.build_unctad reads. A code the table spells differently (France 250
-- vs 251) would silently drop that area's series; returns the missing table × code pairs.
with areas as ({{ poc_unctad_areas() }}),

present as (
    select distinct
        'US_FdiFlowsStock' as entity,
        economy
    from {{ ref('unctad_fdi_flows_stock') }}
    union all
    select distinct
        'US_TradeMerchTotal' as entity,
        economy
    from {{ ref('unctad_merchandise_trade') }}
    union all
    select distinct
        'US_TradeServCatTotal' as entity,
        economy
    from {{ ref('unctad_services_trade') }}
),

tables as (
    select * from (values ('US_FdiFlowsStock'), ('US_TradeMerchTotal'), ('US_TradeServCatTotal')) as t (entity)
)

select
    t.entity,
    a.economy,
    a.candidate_key
from tables as t
cross join areas as a
left join present as p on t.entity = p.entity and a.economy = p.economy
where p.economy is null
