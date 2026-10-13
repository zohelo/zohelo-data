{# NBP exchange rates, tables A/B/C: the newest Landing version of every table-quarter,
   typed. One row per table × effective date × currency. Rates are PLN per 1 unit of the
   currency as published by the API (mid for A/B; bid/ask for C, trading_date for C). #}
with ranked as (
    select *,
           rank() over (partition by entity order by source_version desc, ingested_at desc) as version_rank
    from {{ source('nbp', 'nbp_observations') }}
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
