-- gus_strateg: every SOR indicator dimension with observed values has a value for Poland (level 0, territory
-- 100000000000); every level-2 value is a voivodeship code (10 + an even TERC code 02-32 + 00000000) or Poland; and a
-- year that has voivodeship values has all 16 (Dev trial 45757803813: 21 indicators, 16 in every year). A lost
-- national series, a territory coding that changed or a voivodeship dropped from an answer fails here. One test per
-- Silver model (it reads only this model).
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
union all
select
    'voivodeships in ' || indicator_id || '.' || dimension_id || ' ' || period_year as problem,
    count(distinct territory_code) as found
from {{ ref('gus_strateg_observations') }}
where level_id = '2' and territory_code <> '100000000000'
group by indicator_id, dimension_id, period_year
having count(distinct territory_code) <> 16
