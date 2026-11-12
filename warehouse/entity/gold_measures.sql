-- Gold measures of ONE block (docs/gold-coverage.md): one row per measure of the block's build stage
-- (`build`): the catalogue entry with its unit, frequency, size and period range.
select
    measure_key,
    any_value(source_system) || ':' || any_value(block_id) as dataset_key,
    any_value(source_system) as source_system,
    any_value(block_id) as block_id,
    any_value(block_name) as block_name,
    any_value(measure_name) as measure_name,
    any_value(measure_codes) as measure_codes,
    any_value(unit_code) as unit_key,
    any_value(unit_name) as unit_name,
    any_value(frequency) as frequency,
    min(date_key) as first_date,
    max(date_key) as last_date,
    count(
        distinct geography_key
        || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end
        || '|'
        || breakdown_json
    ) as series,
    count(*) as points,
    count(distinct geography_key) as areas,
    count(*) filter (where geography_key = '_X') as points_geography_unmatched,
    count(*) filter (where is_total) as points_total_series,
    any_value(source_version) as source_version,
    any_value(raw_sha256) as raw_sha256
from build
group by measure_key
