{# NBP exchange rates, tables A/B/C: the newest Landing version of every table-quarter,
   typed, a currency repeated in one table with identical values kept once. One row per
   table × effective date × currency. Rates are PLN per 1 unit of the
   currency as published by the API (mid for A/B; bid/ask for C, trading_date for C). #}
with source_rows as (
    -- Bronze may hold a source row more than once (bronze.nbp_observations did, 15 Oct):
    -- one row per Landing file and row number.
    select *
    from {{ source('nbp', 'nbp_observations') }}
    qualify row_number() over (partition by raw_sha256, source_row_number order by ingested_at) = 1
),

ranked as (
    select
        *,
        rank() over (partition by entity order by source_version desc, ingested_at desc) as version_rank
    from source_rows
    where table_code in ('A', 'B', 'C')
)

select
    table_code,
    table_no,
    cast(effective_date as date) as effective_date,
    cast(trading_date as date) as trading_date,
    upper(currency_code) as currency_code,
    currency_name,
    cast(mid as decimal(18, 8)) as mid_rate,
    cast(bid as decimal(18, 8)) as bid_rate,
    cast(ask as decimal(18, 8)) as ask_rate,
    raw_sha256,
    source_version,
    ingested_at
from ranked
where version_rank = 1
-- Grain is one row per table × effective date × currency. A currency can appear in more
-- than one publication (table_no) of the same quarter; keep the latest publication's row
-- (highest table_no, then latest ingest). Rows that differ in published fields beyond
-- table_no are collapsed to that one row (#803).
qualify row_number() over (
    partition by table_code, effective_date, upper(currency_code)
    order by table_no desc, ingested_at desc, raw_sha256 asc, source_row_number asc
) = 1
