-- Gold: the half-yearly fact holds one row per series and half-year (as tests/fact_grain.sql for the other facts).
select
    series_key,
    date_key,
    count(*) as rows_per_key
from {{ ref('poc_fact_semiannual') }}
group by series_key, date_key
having count(*) > 1
