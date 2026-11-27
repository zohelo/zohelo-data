-- un_sdg: the newest release holds every one of the 27 EU countries with at least 5,000 rows, at least 500 series with
-- a value, Poland (616) with at least 300 series with a value and its under-five mortality (SH_DYN_MORT, both sexes)
-- for every year from 2000 to its last year, and one release only (2026.Q3.G.01: 442,595 rows, 618 series, Dev probe
-- 45935478568). A shortened file, a lost area or a mixed release fails here instead of thinning Gold silently. One test
-- per Silver model (it reads only this model).
with obs as (select * from {{ ref('un_sdg_observations') }}),

pl_u5 as (
    select time_period
    from obs
    where
        series_code = 'SH_DYN_MORT'
        and geo_area_code = '616'
        and value is not null
        and json_extract_string(dimensions, '$.Sex') = 'BOTHSEX'
)

select
    'areas' as problem,
    count(distinct geo_area_code) as found
from obs
having count(distinct geo_area_code) <> 27
union all
select
    'area ' || geo_area_code || ' rows' as problem,
    count(*) as found
from obs
group by geo_area_code
having count(*) < 5000
union all
select
    'series with a value' as problem,
    count(distinct series_code) as found
from obs
where value is not null
having count(distinct series_code) < 500
union all
select
    'Poland series with a value' as problem,
    count(distinct series_code) as found
from obs
where geo_area_code = '616' and value is not null
having count(distinct series_code) < 300
union all
select
    'Poland SH_DYN_MORT years 2000 to last' as problem,
    count(*) as found
from pl_u5
having count(*) = 0 or min(time_period) > 2000 or count(distinct time_period) <> max(time_period) - min(time_period) + 1
union all
select
    'releases' as problem,
    count(distinct release_name) as found
from obs
having count(distinct release_name) <> 1
