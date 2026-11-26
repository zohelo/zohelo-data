{# Gold PoC: one row per time series (measure × geography × breakdown). The conformed
   breakdown keys live here as columns (sex in the PoC); the rest stays in breakdown_json.
   series_key is readable: measure_key|geography_key|breakdown_json (an unmatched area keeps
   its source code in the key); source totals T become _T (docs/model.md).
   Incremental per Gold block (macros/poc_incremental.sql): a series belongs to one block (its measure_key starts
   with its source_system, and a source has one block), so grouping by the block too changes no row, and a series
   that two blocks emitted would show as two rows and fail the unique test on series_key instead of being merged
   from one block. #}
{{
    config(
        alias='dim_series',
        materialized='incremental',
        incremental_strategy='gold_blocks',
        on_schema_change='fail',
        full_refresh=poc_block_full_refresh('poc_dim_series'),
    )
}}
with keyed as (
    select
        {{ poc_series_key() }} as series_key,
        *
    from ({{ poc_observations(is_incremental()) }})
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
    any_value(geography_source_code) as geography_source_code,
    gold_block
from keyed
group by series_key, gold_block
