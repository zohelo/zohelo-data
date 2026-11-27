-- wipo_ip_stats: the newest design file holds both measures, each for at least 120 offices (140 in the file of June
-- 2024, Dev probe 45872152431), the EUIPO (EM) in every year from 2008 to the last year, the Polish office (PL) in the
-- last year, a last year no older than two years before the file's version year, and one file only. One test per
-- Silver model (it reads only this model).
with obs as (select * from {{ ref('wipo_ip_stats_designs') }}),

measures as (
    select
        m.measure,
        count(distinct o.office_code) as offices,
        count(distinct o.year) filter (where o.office_code = 'EM') as em_years,
        min(o.year) filter (where o.office_code = 'EM') as em_first,
        max(o.year) filter (where o.office_code = 'EM') as em_last,
        max(o.year) filter (where o.office_code = 'PL') as pl_last,
        max(o.year) as last_year
    from (values ('applications'), ('registrations')) as m (measure)
    left join obs as o on m.measure = o.measure
    group by m.measure
),

totals as (
    select
        count(distinct raw_sha256) as files,
        max(year) as last_year,
        cast(left(max(source_version), 4) as integer) as version_year
    from obs
)

select measure || ': offices' as problem, offices as found from measures where offices < 120
union all
select measure || ': EM years (2008 to the last year, no gap)', em_years from measures
where coalesce(em_first, 0) <> 2008 or coalesce(em_last, 0) <> last_year or em_years <> coalesce(em_last - em_first + 1, -1)
union all
select measure || ': PL office not in the last year', pl_last from measures where coalesce(pl_last, 0) <> last_year
union all
select 'files', files from totals where files <> 1
union all
select 'last year older than version year - 2', last_year from totals where last_year < version_year - 2
