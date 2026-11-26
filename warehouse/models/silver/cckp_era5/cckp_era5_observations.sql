{# CCKP ERA5 (register GL-036): ERA5 0.25-degree reanalysis aggregated by the World Bank over the land of each country
   (all_countries answers, ISO3 codes) and of Poland's 16 first-level units (CCKP codes POL.1982370-POL.1982385), from
   the newest copy of entity era5 (bronze.cckp_era5_records). Variables: tas, mean air
   temperature in degC (monthly: the month's mean; annual: the year's mean as CCKP computes it, within 0.15 degC of the
   plain average of the twelve months, Dev trial 45826698285), and pr, precipitation in mm (monthly: the
   month's total; annual: the year's total, the sum of the twelve months: Poland 2024 749.89 = sum of its monthly
   values, Dev probe 45810479823). CCKP prints no unit in the answer; the units are those of the CCKP variables.
   Reanalysis estimates, not station observations and not projections. Periods as published: monthly 'YYYY-MM',
   annual 'YYYY-07' (the year, keyed at mid-year). value is the published number; a published null or text that is
   not a number keeps value null (value_text as published) and never becomes 0. CCKP prints no markers: no null and no
   text among 517,712 values, no gaps (Dev trial 45826698285); its zeros are real (1,237 monthly pr totals of 0.0 mm in
   deserts, e.g. QAT 1953-06; 31 tas values of 0.0 degC); the annual pr totals equal the sum of the months within
   0.04 mm for all 19,912 area-years. dataset_period is the period of the dataset id read (e.g. 1950-2025).
   Grain: variable x aggregation x geocode x period_label. #}
with records as (
    select *
    from {{ source('cckp_era5', 'cckp_era5_records') }}
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
