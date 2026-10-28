{{ config(alias='build_check') }}
{# Gold PoC build check, one row per activated block (also those that yielded no rows): rows,
   series, unmatched geography (_X), unplaced periods, dates, status flags. Read it after
   every build. #}
with activated as ({{ poc_activated_blocks() }}),

built as (
    select
        source_system,
        block_id,
        count(*) as row_count,
        count(distinct {{ poc_series_key() }}) as series,
        count(distinct measure_key) as measures,
        count(*) filter (where geography_key = '_X') as rows_geography_unmatched,
        (list(distinct geography_source_code) filter (where geography_key = '_X'))[1:10] as unmatched_sample,
        count(*) filter (where is_total) as rows_total_series,
        count(*) filter (where frequency = 'X') as rows_period_unplaced,
        (list(distinct period_label) filter (where frequency = 'X'))[1:6] as unplaced_period_sample,
        min(date_key) as first_date,
        max(date_key) as last_date,
        list(distinct frequency) as frequencies,
        list(distinct observation_status_key) as statuses
    from ({{ poc_observations() }})
    group by source_system, block_id
),

catalogue as (  -- what the catalogue says about a block that yielded nothing
    select
        source_system,
        block_id,
        min(status) as status
    from {{ ref('poc_dim_measure') }}
    group by source_system, block_id
)

select
    a.source_system,
    a.block_id,
    a.block_name,
    coalesce(b.row_count, 0) as row_count,
    coalesce(b.series, 0) as series,
    coalesce(b.measures, 0) as measures,
    coalesce(b.rows_geography_unmatched, 0) as rows_geography_unmatched,
    b.unmatched_sample,
    coalesce(b.rows_total_series, 0) as rows_total_series,
    coalesce(b.rows_period_unplaced, 0) as rows_period_unplaced,
    b.unplaced_period_sample,
    b.first_date,
    b.last_date,
    b.frequencies,
    b.statuses,
    case when b.row_count is null then coalesce(c.status, 'no rows: not in Silver or filtered out') end as note
from activated as a
left join built as b on a.source_system = b.source_system and a.block_id = b.block_id
left join catalogue as c on a.source_system = c.source_system and a.block_id = c.block_id
