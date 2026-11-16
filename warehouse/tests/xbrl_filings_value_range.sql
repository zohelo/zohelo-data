-- xbrl_filings build stage (gold.build_xbrl_filings): every count is a whole number between 1 and 1,500 a year, and a
-- year never has more issuers than filings. Poland has about 400 issuers on the regulated market and the repository
-- holds at most a few hundred filings a year (Dev trial, docs/sources/xbrl_filings.md); a value above the bound means
-- filings counted several times (a list read twice, another country), a fraction or a zero a cell that is not a
-- count. A live test on the build stage: no KPI reads this source. Returns the rows outside.
with build as (
    select * from {{ ref('poc_build_xbrl_filings') }}
)

select
    measure_key,
    date_key,
    value,
    'out of range' as reason
from build
where value < 1 or value > 1500 or value <> floor(value)
union all
select
    i.measure_key,
    i.date_key,
    i.value,
    'more issuers than filings' as reason
from build as i
inner join build as f
    on i.date_key = f.date_key and f.measure_key = 'xbrl_filings:esef_reports:filings'
where i.measure_key = 'xbrl_filings:esef_reports:entities:issuers' and i.value > f.value
