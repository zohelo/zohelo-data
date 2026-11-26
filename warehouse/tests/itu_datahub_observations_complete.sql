-- itu_datahub: the newest ITU_DH file holds at least 39 indicators (40 named by the Data360 indicator list, Dev
-- probe 45871438953), the internet-use total (IT_NET_USER, PT_POP, sex, age and urbanisation '_T') for at least 200
-- economies, Poland's series of it from 2000 without a gap to a year no older than three years before today, one file
-- version, and a number in every row whose unit is not CAT (the IT_COMP_* text indicators). A shortened file, a lost
-- indicator, a gap in Poland's series, a mixed version or a value that is not a number fails here instead of thinning
-- Gold silently. One test per Silver model (it reads only this model).
with obs as (select * from {{ ref('itu_datahub_observations') }}),

users as (
    select *
    from obs
    where
        indicator = 'IT_NET_USER' and unit_measure = 'PT_POP' and sex = '_T' and age = '_T' and urbanisation = '_T'
        and value is not null
)

select
    'indicators' as problem,
    count(distinct indicator) as found
from obs
having count(distinct indicator) < 39
union all
select
    'economies with IT_NET_USER total' as problem,
    count(distinct ref_area) as found
from users
having count(distinct ref_area) < 200
union all
select
    'POL IT_NET_USER total years (2000 to the last year, no gap)' as problem,
    count(distinct year) as found
from users
where ref_area = 'POL'
having
    count(distinct year) = 0
    or min(year) <> 2000
    or count(distinct year) <> max(year) - 2000 + 1
    or max(year) < year(current_date) - 3
union all
select
    'file versions' as problem,
    count(distinct source_version) as found
from obs
having count(distinct source_version) <> 1
union all
select
    'rows without a number outside unit CAT' as problem,
    count(*) as found
from obs
where value is null and unit_measure <> 'CAT'
having count(*) > 0
