{# NBP exchange rates, tables A/B/C: the newest Landing version of every table-quarter,
   typed. One row per table × effective date × currency; when the newest version holds
   more than one published value set for a key, published_variants > 1 and its test
   reports it (never silent, #803). Rates are PLN per 1 unit of the
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
),

current_rows as (
    -- Distinct published value sets per key in the newest version: 1 = repeats only.
    select
        *,
        dense_rank() over (
            partition by table_code, effective_date, upper(currency_code)
            order by table_no, trading_date, currency_name, mid, bid, ask
        ) as variant
    from ranked
    where version_rank = 1
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
    ingested_at,
    max(variant) over (partition by table_code, effective_date, upper(currency_code)) as published_variants
from current_rows
-- Grain is one row per table × effective date × currency: keep the row of the highest
-- table_no, then latest ingest. Repeats equal in every published field collapse silently;
-- differing rows are counted in published_variants and fail its test (#803, #852 review).
qualify row_number() over (
    partition by table_code, effective_date, upper(currency_code)
    order by table_no desc, ingested_at desc, raw_sha256 asc, source_row_number asc
) = 1
