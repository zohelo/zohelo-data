-- World Bank GEM: the series landed (SERIES in ingestion/to_landing/world_bank_gem/extract.py, seen in Silver) and
-- the Gold blocks (macro poc_world_bank_gem_series) must be the same list, so a series added to one place is not
-- landed without reaching Gold, or declared in Gold without being landed. Returns the series in only one of them.
with landed as (
    select distinct series_code from {{ ref('gem_observations') }}
),

built as (
    select series_code from ({{ poc_world_bank_gem_series() }})
)

select
    coalesce(l.series_code, b.series_code) as series_code,
    case when l.series_code is null then 'in Gold blocks, not landed' else 'landed, not a Gold block' end as problem
from landed as l
full outer join built as b on l.series_code = b.series_code
where l.series_code is null or b.series_code is null
