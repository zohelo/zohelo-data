{{ config(alias='grain_check') }}
{# Gold PoC: duplicate series × period rows, by source, with the source codes behind them
   (two source units mapped to one area, or a block counted twice). Empty when the grain holds;
   tests/poc_fact_grain.sql fails when it is not. #}
with observations as ({{ poc_observations() }}),

duplicates as (
    select
        source_system,
        block_id,
        {{ poc_series_key() }} as series_key,
        date_key,
        count(*) as rows_per_key,
        list(distinct geography_source_code) as source_codes,
        list(distinct coalesce(cast(value as varchar), 'null')) as values_seen
    from observations
    group by source_system, block_id, series_key, date_key
    having count(*) > 1
)

select
    source_system,
    block_id,
    count(*) as duplicate_keys,
    sum(rows_per_key) as duplicate_rows,
    (list(series_key order by series_key))[1:3] as series_sample,
    (list(source_codes order by series_key))[1:3] as source_codes_sample,
    (list(values_seen order by series_key))[1:3] as values_sample
from duplicates
group by source_system, block_id
