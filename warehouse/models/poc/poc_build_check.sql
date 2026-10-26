{{ config(alias='build_check') }}
{# Gold PoC build check, one row per activated block: rows, series, unmatched geography
   (_X), rows without a value date, status flags. Read it after every build. #}
select
    source_system,
    block_id,
    any_value(block_name) as block_name,
    count(*) as row_count,
    count(distinct {{ poc_series_key() }}) as series,
    count(distinct measure_key) as measures,
    count(*) filter (where geography_key = '_X') as rows_geography_unmatched,
    (list(distinct geography_source_code) filter (where geography_key = '_X'))[1:10] as unmatched_sample,
    count(*) filter (where is_total) as rows_total_series,
    min(date_key) as first_date,
    max(date_key) as last_date,
    list(distinct frequency) as frequencies,
    list(distinct observation_status_key) as statuses
from ({{ poc_observations() }})
group by source_system, block_id
