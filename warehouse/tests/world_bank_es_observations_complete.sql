-- world_bank_es: the newest WB_ES file holds at least 550 indicators (561 in Dev probe 45924752018), the all-firms
-- total of bribery incidence (WB_ES_T_GRAFT3, every breakdown '_T') for at least 170 economies (181), Poland's four
-- survey years 2009, 2013, 2019 and 2025 of it, one file version, and a number in every row. A shortened file, a lost
-- indicator or survey, a mixed version or a value that is not a number fails here instead of thinning Gold silently.
-- One test per Silver model (it reads only this model).
with obs as (select * from {{ ref('world_bank_es_observations') }}),

bribery as (
    select *
    from obs
    where
        indicator = 'WB_ES_T_GRAFT3' and comp_breakdown_1 = '_T' and comp_breakdown_2 = '_T'
        and comp_breakdown_3 = '_T' and value is not null
)

select
    'indicators' as problem,
    count(distinct indicator) as found
from obs
having count(distinct indicator) < 550
union all
select
    'economies with WB_ES_T_GRAFT3 total' as problem,
    count(distinct ref_area) as found
from bribery
having count(distinct ref_area) < 170
union all
select
    'POL WB_ES_T_GRAFT3 total in 2009, 2013, 2019 and 2025' as problem,
    count(distinct year) as found
from bribery
where ref_area = 'POL' and year in (2009, 2013, 2019, 2025)
having count(distinct year) <> 4
union all
select
    'file versions' as problem,
    count(distinct source_version) as found
from obs
having count(distinct source_version) <> 1
union all
select
    'rows without a number' as problem,
    count(*) as found
from obs
where value is null
having count(*) > 0
