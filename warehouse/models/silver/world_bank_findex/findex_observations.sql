{# Global Findex country-level indicators of the newest edition, typed: one row per economy or aggregate
   × survey year × population group × indicator. Values are shares of the population group (0-1), e.g.
   account_t_d = account ownership, age 15+. Aggregates are World Bank regions and income groups (EAP,
   ECA, HIC, …); no EU aggregate. #}
with newest as (
    select * from {{ source('world_bank_findex', 'findex_observations') }}
    where
        source_version
        = (select max(latest.source_version) from {{ source('world_bank_findex', 'findex_observations') }} as latest)
)

select
    trim(country_code) as economy_code,
    country_name as economy_name,
    cast(year as integer) as year,
    group_name,
    group_value,
    indicator,
    try_cast(value_text as double) as value,
    value_text,
    region,
    income_group,
    try_cast(nullif(pop_adult, 'NA') as bigint) as pop_adult,
    raw_sha256,
    source_version
from newest
