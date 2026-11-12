-- Gold series of ONE block (docs/gold-coverage.md): one row per time series of the block's build stage
-- (`build`), with the columns of gold.dim_series plus the block's version. breakdown_label_json is
-- whatever the build stage gives (empty for Eurostat: names of breakdown codes are not repeated per series).
select
    measure_key
    || '|'
    || geography_key
    || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end
    || '|'
    || breakdown_json as series_key,
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
    any_value(geography_source_code) as geography_source_code,
    any_value(source_version) as source_version,
    any_value(raw_sha256) as raw_sha256
from build
group by all
