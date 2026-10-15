{# NBP gold price (PLN per gram of 1000 fineness gold), newest Landing version of every
   quarter, typed. One row per effective date. #}
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
    where table_code = 'GOLD'
)

select
    cast(effective_date as date) as effective_date,
    cast(gold_price as decimal(18, 4)) as price_pln_per_gram,
    raw_sha256,
    source_version,
    ingested_at
from ranked
where version_rank = 1
