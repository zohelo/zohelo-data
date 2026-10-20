{{ config(materialized='incremental', incremental_strategy='append',
          full_refresh=('fact_observation' in var('full_refresh_models', [])) or none,
          pre_hook="{% if is_incremental() %}{{ fact_observation_clear() }}{% endif %}") }}
{# Statistical observations at their true grain: indicator × reference area × reference
   period, one value, tagged with its source. v1: World Bank WDI (annual).
   Keys: indicator_key → dim_indicator, geography_key → dim_geography,
   period_key → dim_period. Eurostat and BDL observations join here as they are modelled.
   Incremental per source input file (owner policy #822, #870): the pre-hook deletes rows
   whose (source_system, raw_sha256) is no longer current; the build inserts the current
   recipes Gold lacks, so a new WDI archive rewrites only WDI's rows and a failed run is
   repaired by the next one. Recipe rule and null-safety: macros/fact_observation.sql. #}
with wdi as (
    select
        o.*,
        cast(strptime(o.source_version[1:8], '%Y%m%d') as date) as vintage
    from {{ ref('wdi_observations') }} as o
    where
        o.value is not null
        {% if is_incremental() %}
            and 'world_bank_wdi|' || coalesce(o.raw_sha256, '') not in (
                select distinct h.source_system || '|' || coalesce(h.raw_sha256, '')
                from {{ this }} as h
            )
        {% endif %}
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
