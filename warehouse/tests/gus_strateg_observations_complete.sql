-- gus_strateg: every SOR indicator dimension with observed values has a value for Poland (level 0, territory
-- 100000000000), and every level-2 value is a voivodeship code (10 + an even TERC code 02-32 + 00000000) or Poland. A lost
-- national series or a territory coding that changed fails here. One test per Silver model (it reads only this model).
select
    'no value for Poland: ' || indicator_id || '.' || dimension_id as problem,
    count(*) as found
from {{ ref('gus_strateg_observations') }}
group by indicator_id, dimension_id
having count(*) filter (where level_id = '0' and territory_code = '100000000000') = 0
union all
select
    'level 2 code not a voivodeship: ' || territory_code as problem,
    count(*) as found
from {{ ref('gus_strateg_observations') }}
where
    level_id = '2'
    and not regexp_full_match(territory_code, '10(00|0[2468]|[12][02468]|3[02])00000000')
group by territory_code
