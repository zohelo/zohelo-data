{# gold.fact_observation holds exactly the current recipes (incremental model, #822, #870):
   every current source input file with a non-null value, and nothing else. A failure lists
   the recipes Gold lacks ("missing") or holds although not current ("stale"). #}
with {{ fact_observation_recipes() }},

held as (
    select distinct
        source_system,
        coalesce(raw_sha256, '') as raw_sha256
    from {{ ref('fact_observation') }}
)

select
    'missing' as problem,
    r.*
from recipes as r
where not exists (
    select 1 from held as h
    where h.source_system = r.source_system and h.raw_sha256 = r.raw_sha256
)
union all
select
    'stale' as problem,
    h.*
from held as h
where not exists (
    select 1 from recipes as r
    where h.source_system = r.source_system and h.raw_sha256 = r.raw_sha256
)
