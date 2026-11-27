{{ config(alias='build_cckp_era5', tags=['gold_block']) }}
{# Gold build stage: World Bank CCKP ERA5 0.25-degree (register GL-036), block 'era5', from
   silver.cckp_era5_observations. Four measures, one frequency each: measure_key cckp_era5:era5:<unit>:<var>.<agg>,
   cckp_era5:era5:degC:tas.monthly (M) and tas.annual (A), mean air temperature; cckp_era5:era5:mm:pr.monthly (M) and
   pr.annual (A), precipitation total. Values and units as published (D33): reanalysis estimates over each area's
   land, not station observations (IMGW stays its own source) and not projections. Areas through poc_map_geography
   (section cckp_era5, static): countries by ISO3, Poland's 16 first-level units by the stated code map; a CCKP
   country code without a Gold country maps to _X with its code kept and is counted by build_check. Values that are
   not numbers (value null) are not loaded; a period after the build day cannot be an observation and is left out. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'cckp_era5'
)

select
    'cckp_era5' as source_system,
    'era5' as block_id,
    'World Bank CCKP: ERA5 0.25-degree air temperature and precipitation' as block_name,
    'cckp_era5:era5:' || o.unit_code || ':' || o.variable || '.' || o.aggregation as measure_key,
    case o.variable
        when 'tas' then 'Mean air temperature, ' || o.aggregation || ' mean (ERA5 0.25°, World Bank CCKP)'
        else 'Precipitation, ' || o.aggregation || ' total (ERA5 0.25°, World Bank CCKP)'
    end as measure_name,
    o.variable || '.' || o.aggregation as measure_codes,
    o.unit_code,
    case o.unit_code when 'degC' then 'degrees Celsius' else 'millimetres' end as unit_name,
    o.geocode as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    case o.aggregation when 'monthly' then 'M' else 'A' end as frequency,
    make_date(o.period_year, coalesce(o.period_month, 1), 1) as date_key,
    case o.aggregation when 'monthly' then o.period_label else cast(o.period_year as varchar) end as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from {{ ref('cckp_era5_observations') }} as o
left join geo as g on o.geocode = g.source_code
where
    o.value is not null
    and make_date(o.period_year, coalesce(o.period_month, 1), 1) <= current_date
