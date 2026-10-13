{# NBP gold price (PLN per gram of 1000 fineness gold), newest Landing version of every
   quarter, typed. One row per effective date. #}
with ranked as (
    select *,
           rank() over (partition by entity order by source_version desc, ingested_at desc) as version_rank
    from {{ source('nbp', 'nbp_observations') }}
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
