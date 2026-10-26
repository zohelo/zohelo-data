{{ config(alias='dim_series') }}
{# Gold PoC: one row per time series (measure × geography × breakdown). The conformed
   breakdown keys live here as columns (sex in the PoC); the rest stays in breakdown_json.
   series_key is readable: measure_key|geography_key|breakdown_json. #}
select
    {{ poc_series_key() }} as series_key,
    measure_key,
    geography_key,
    source_system,
    block_id,
    any_value(frequency) as frequency,
    any_value(unit_code) as unit_key,
    breakdown_json,
    any_value(breakdown_label_json) as breakdown_label_json,
    coalesce(any_value(sex_code), '_Z') as sex_key,
    any_value(is_total) as is_total,
    min(date_key) as first_date,
    max(date_key) as last_date,
    count(*) as points,
    any_value(geography_source_code) as geography_source_code
from ({{ poc_observations() }})
group by measure_key, geography_key, breakdown_json, source_system, block_id
