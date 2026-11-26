-- irena_stat: the newest release holds both measures for at least 200 countries or areas (224 in release 2026 H2,
-- Dev probe 45866615504), Poland (POL) with Total Renewable capacity and generation of every grid total ('All') from
-- 2000 to the release's last year, a last year no older than two years before the release year, and one release only.
-- A shortened answer, a lost measure, a gap in Poland's series or a mixed release fails here instead of thinning Gold
-- silently. One test per Silver model (it reads only this model).
with obs as (select * from {{ ref('irena_stat_electricity') }}),

releases as (
    select
        count(distinct release) as n,
        cast(left(min(release), 4) as integer) as release_year,
        max(year) filter (where value is not null) as last_year
    from obs
)

select
    'areas with ' || measure as problem,
    count(distinct country_code) as found
from obs
where value is not null
group by measure
having count(distinct country_code) < 200
union all
select
    'measures with values' as problem,
    count(distinct measure) as found
from obs
where value is not null
having count(distinct measure) <> 2
union all
select
    'POL Total Renewable ' || m.measure || ' years (2000 to the measure''s last year)' as problem,
    count(o.year) as found
from (values ('capacity'), ('generation')) as m (measure)
left join obs as o
    on
        o.measure = m.measure
        and o.country_code = 'POL'
        and o.technology = 'Total Renewable'
        and o.grid_connection = 'All'
        and o.value is not null
group by m.measure
having
    count(o.year) = 0
    or min(o.year) <> 2000
    or count(distinct o.year) <> max(o.year) - 2000 + 1
    or max(o.year) < (select max(x.year) from obs as x where x.measure = m.measure and x.value is not null) - 1
union all
select
    'releases' as problem,
    n as found
from releases
where n <> 1
union all
select
    'last year older than release year - 2' as problem,
    last_year as found
from releases
where last_year < release_year - 2
