-- cckp_era5: every variable x aggregation holds at least 240 countries (246 in Dev probe 45810479823) and Poland's 16
-- first-level units; every series runs without a gap from 1950 to the last year of the dataset id, with 12 months a
-- year (monthly) or one 'YYYY-07' key a year (annual); one dataset period only. A shortened answer, a lost
-- voivodeship, a gap or a changed period key fails here instead of thinning Gold silently. One test per Silver model
-- (it reads only this model). The first-level codes must be exactly the stated voivodeship map
-- (poc_cckp_era5_voivodeships() in macros/sources/cckp_era5.sql, read by the Gold geography map).
with obs as (select * from {{ ref('cckp_era5_observations') }}),

stated as ({{ poc_cckp_era5_voivodeships() }}),

series as (
    select
        variable,
        aggregation,
        geocode,
        count(*) as periods,
        min(period_year) as first_year,
        max(period_year) as last_year,
        cast(right(max(dataset_period), 4) as integer) as dataset_last
    from obs
    group by all
)

select
    'areas in ' || variable || ' ' || aggregation || ' ' || geo_level as problem,
    count(distinct geocode) as found
from obs
group by variable, aggregation, geo_level
having count(distinct geocode) < case geo_level when 'country' then 240 else 16 end
    or (geo_level = 'adm1' and count(distinct geocode) filter (where geocode like 'POL.%') <> 16)
union all
select
    'variable x aggregation without POL' as problem,
    count(*) as found
from (select distinct variable, aggregation from obs) as v
where not exists (
    select 1 from obs as o
    where o.variable = v.variable and o.aggregation = v.aggregation and o.geocode = 'POL'
)
having count(*) > 0
union all
select
    'gap or short series: ' || variable || ' ' || aggregation || ' ' || geocode as problem,
    periods as found
from series
where
    first_year <> 1950
    or last_year <> dataset_last
    or periods <> (last_year - first_year + 1) * case aggregation when 'monthly' then 12 else 1 end
union all
select
    'annual period key not YYYY-07: ' || min(period_label) as problem,
    count(*) as found
from obs
where aggregation = 'annual' and right(period_label, 3) <> '-07'
having count(*) > 0
union all
select
    'first-level code not in the stated map: ' || o.geocode as problem,
    count(*) as found
from obs as o
anti join stated as s on o.geocode = s.geocode
where o.geo_level = 'adm1'
group by o.geocode
union all
select
    'stated code without data: ' || s.geocode as problem,
    1 as found
from stated as s
anti join obs as o on s.geocode = o.geocode
union all
select
    'dataset periods' as problem,
    count(distinct dataset_period) as found
from obs
having count(distinct dataset_period) <> 1
