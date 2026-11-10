{# Official NBP interest rates (reference, lombard, deposit, rediscount, discount): one row per
   rate and effective date of an MPC change, from the newest Landing copy of the archive file
   (rates_archive, history from 1998-02-26). A rate holds from effective_from until the next row
   of the same rate_code (valid_to, exclusive; null = in force). Names (PL/EN) come from the
   current file (rates_current, table stoproc). Percent per year, decimal comma converted. #}
with source_rows as (
    select *
    from {{ source('nbp_interest_rates', 'nbp_interest_rate_items') }}
    qualify row_number() over (partition by raw_sha256, source_row_number order by ingested_at) = 1
),

newest as (
    select *
    from source_rows
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

names as (
    select
        item_id as rate_code,
        any_value(name_pl) as name_pl,
        any_value(name_en) as name_en
    from newest
    where entity = 'rates_current' and group_tag = 'tabela' and group_id = 'stoproc'
    group by item_id
),

changes as (
    select
        item_id as rate_code,
        cast(effective_from as date) as effective_from,
        cast(replace(rate_text, ',', '.') as decimal(6, 2)) as rate_pct,
        raw_sha256,
        source_version,
        ingested_at
    from newest
    where entity = 'rates_archive' and group_tag = 'pozycje'
)

select
    c.rate_code,
    n.name_pl,
    n.name_en,
    c.effective_from,
    lead(c.effective_from) over (partition by c.rate_code order by c.effective_from) as valid_to,
    c.rate_pct,
    c.raw_sha256,
    c.source_version,
    c.ingested_at
from changes as c
left join names as n on c.rate_code = n.rate_code
