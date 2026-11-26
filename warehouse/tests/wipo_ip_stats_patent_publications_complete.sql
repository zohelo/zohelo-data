-- wipo_ip_stats: the newest patent file holds publications for at least 80 offices and 190 origins (93 and 204 in
-- the file of April 2024, Dev probe 45872152431), every technology field 0-35, the Polish office (PL) in every year
-- from 2000 to the last year, a last year no older than two years before the file's version year, and one file only.
-- A shortened file, a lost field, a gap in Poland's series or two mixed files fails here instead of thinning Gold
-- silently. One test per Silver model (it reads only this model).
with obs as (select * from {{ ref('wipo_ip_stats_patent_publications') }}),

totals as (
    select
        count(distinct office_code) as offices,
        count(distinct origin_code) as origins,
        count(distinct technology_field_code) as fields,
        count(distinct raw_sha256) as files,
        max(year) as last_year,
        cast(left(max(source_version), 4) as integer) as version_year,
        count(distinct year) filter (where office_code = 'PL') as pl_years,
        min(year) filter (where office_code = 'PL') as pl_first,
        max(year) filter (where office_code = 'PL') as pl_last
    from obs
)

select 'offices' as problem, offices as found from totals where offices < 80
union all
select 'origins', origins from totals where origins < 190
union all
select 'technology fields', fields from totals where fields <> 36
union all
select 'files', files from totals where files <> 1
union all
select 'last year older than version year - 2', last_year from totals where last_year < version_year - 2
union all
select 'PL office years (2000 to the last year, no gap)', pl_years from totals
where coalesce(pl_first, 0) <> 2000 or coalesce(pl_last, 0) <> last_year or pl_years <> coalesce(pl_last - pl_first + 1, -1)
