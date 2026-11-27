-- gios_groundwater: every year from the first to the last has results, and an inorganic file with nitrate results;
-- a year holds one monitoring kind (Gold's nitrate measures are per kind; two kinds in one year need a decision); no
-- nitrate limit of quantification above 50 mg/l (a '<' result would then not tell whether the point is above the
-- 50 mg/l threshold Gold counts). One test per Silver model (it reads only this model).
with years as (
    select range as survey_year
    from range(
        (select min(survey_year) from {{ ref('gios_groundwater_results') }}),
        (select max(survey_year) from {{ ref('gios_groundwater_results') }}) + 1
    )
)

select
    'no nitrate results in ' || y.survey_year as problem,
    count(r.point_key) as found
from years as y
left join {{ ref('gios_groundwater_results') }} as r
    on y.survey_year = r.survey_year and r.indicator_key = 'azotany[mgno3/l]'
group by y.survey_year
having count(r.point_key) = 0
union all
select
    'monitoring kinds in ' || survey_year as problem,
    count(distinct monitoring_kind) as found
from {{ ref('gios_groundwater_results') }}
group by survey_year
having count(distinct monitoring_kind) > 1
union all
select
    'nitrate limit above 50 mg/l: ' || point_key || ' ' || sample_date as problem,
    count(*) as found
from {{ ref('gios_groundwater_results') }}
where indicator_key = 'azotany[mgno3/l]' and below_limit and limit_value > 50
group by point_key, sample_date
