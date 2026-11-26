{# CCKP ERA5 (register GL-036): ERA5 0.25-degree reanalysis aggregated by the World Bank over the land of each country
   (all_countries answers, ISO3 codes) and of Poland's 16 first-level units (CCKP codes POL.1982370-POL.1982385), from
   the newest copy of entity era5 (bronze.cckp_era5_records, record_kind 'value'). Variables: tas, mean air
   temperature in degC (monthly: the month's mean; annual: the year's mean), and pr, precipitation in mm (monthly: the
   month's total; annual: the year's total, the sum of the twelve months: Poland 2024 749.89 = sum of its monthly
   values, Dev probe 45810479823). CCKP prints no unit in the answer; the units are those of the CCKP variables.
   Reanalysis estimates, not station observations and not projections. Periods as published: monthly 'YYYY-MM',
   annual 'YYYY-07' (the year, keyed at mid-year). value is the published number; a published null or text that is
   not a number keeps value null (value_text as published) and never becomes 0; CCKP leaves no gaps as far as seen
   (Dev trial). dataset_period is the period of the dataset id read (e.g. 1950-2025). Grain: variable x aggregation x
   geocode x period. #}
with records as (
    select *
    from {{ source('cckp_era5', 'cckp_era5_records') }}
    where record_kind = 'value'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    variable,
    aggregation,
    geocode,
    case when geocode like '%.%' then 'adm1' else 'country' end as geo_level,
    period as period_label,
    cast(left(period, 4) as integer) as period_year,
    case when aggregation = 'monthly' then cast(right(period, 2) as integer) end as period_month,
    case when regexp_full_match(value, '-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?') then cast(value as double) end as value,
    value as value_text,
    case variable when 'tas' then 'degC' when 'pr' then 'mm' end as unit_code,
    dataset_period,
    raw_sha256,
    source_version
from records
