{# Statistical observations at their true grain: indicator × reference area × reference
   period, one value, tagged with its source. v1: World Bank WDI (annual).
   Keys: indicator_key → dim_indicator, geography_key → dim_geography,
   period_key → dim_period. Eurostat and BDL observations join here as they are modelled. #}
with wdi as (
    select
        o.*,
        cast(strptime(o.source_version[1:8], '%Y%m%d') as date) as vintage
    from {{ ref('wdi_observations') }} as o
    where o.value is not null
)

select
    'WDI:' || indicator_code as indicator_key,
    'WB:' || economy_code || ':' || strftime(vintage, '%Y%m%d') as geography_key,
    cast(year as varchar) as period_key,
    make_date(year, 1, 1) as period_start_date,
    'A' as frequency_code,
    value,
    'world_bank_wdi' as source_system,
    raw_sha256
from wdi
