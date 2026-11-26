{# Gold PoC: one row per time series (measure × geography × breakdown). The conformed
   breakdown keys live here as columns (sex in the PoC); the rest stays in breakdown_json.
   series_key is readable: measure_key|geography_key|breakdown_json (an unmatched area keeps
   its source code in the key); source totals T become _T (docs/model.md).
   Incremental per Gold block (macros/poc_incremental.sql): a series belongs to one block (its measure_key starts
   with its source_system, and a source has one block), so grouping by the block too changes no row, and a series
   that two blocks emitted would show as two rows and fail the unique test on series_key instead of being merged
   from one block. Every column is cast to its type: an incremental run reads only the changed blocks, and their
   own column types must not decide the table's (on_schema_change='fail'). #}
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
    cast(series_key as varchar) as series_key,
    cast(any_value(measure_key) as varchar) as measure_key,
    cast(any_value(geography_key) as varchar) as geography_key,
    cast(any_value(source_system) as varchar) as source_system,
    cast(any_value(block_id) as varchar) as block_id,
    cast(any_value(frequency) as varchar) as frequency,
    cast(any_value(unit_code) as varchar) as unit_key,
    cast(any_value(breakdown_json) as varchar) as breakdown_json,
    cast(any_value(breakdown_label_json) as varchar) as breakdown_label_json,
    cast(
        case any_value(sex_code) when 'T' then '_T' else coalesce(any_value(sex_code), '_Z') end as varchar
    ) as sex_key,
    cast(any_value(is_total) as boolean) as is_total,
    cast(min(date_key) as date) as first_date,
    cast(max(date_key) as date) as last_date,
    cast(count(*) as bigint) as points,
    cast(any_value(geography_source_code) as varchar) as geography_source_code,
    cast(gold_block as varchar) as gold_block
from keyed
group by series_key, gold_block
