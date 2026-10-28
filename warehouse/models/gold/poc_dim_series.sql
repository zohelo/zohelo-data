{{ config(alias='dim_series') }}
{# Gold PoC: one row per time series (measure × geography × breakdown). The conformed
   breakdown keys live here as columns (sex in the PoC); the rest stays in breakdown_json.
   series_key is readable: measure_key|geography_key|breakdown_json (an unmatched area keeps
   its source code in the key); source totals T become _T (docs/model.md). #}
with keyed as (
    select
        {{ poc_series_key() }} as series_key,
        *
    from ({{ poc_observations() }})
)

select
    series_key,
    any_value(measure_key) as measure_key,
    any_value(geography_key) as geography_key,
    any_value(source_system) as source_system,
    any_value(block_id) as block_id,
    any_value(frequency) as frequency,
    any_value(unit_code) as unit_key,
    any_value(breakdown_json) as breakdown_json,
    any_value(breakdown_label_json) as breakdown_label_json,
    case any_value(sex_code) when 'T' then '_T' else coalesce(any_value(sex_code), '_Z') end as sex_key,
    any_value(is_total) as is_total,
    min(date_key) as first_date,
    max(date_key) as last_date,
    count(*) as points,
    any_value(geography_source_code) as geography_source_code
from keyed
group by series_key
