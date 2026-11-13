-- ESMA registers (PLG-016): every entry of the newest Bronze copy of the Polish slice yields exactly one Silver row,
-- and Silver holds no entry that copy does not hold. Returns the entry ids that are missing, repeated or extra.
with bronze as (
    select id as entry_id
    from {{ source('esma_registers', 'esma_registers_entries') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

silver as (
    select
        entry_id,
        count(*) as n
    from {{ ref('esma_registers_entities') }}
    group by entry_id
)

select
    coalesce(b.entry_id, s.entry_id) as entry_id,
    coalesce(s.n, 0) as silver_rows,
    b.entry_id is not null as in_bronze
from bronze as b
full outer join silver as s on b.entry_id = s.entry_id
where b.entry_id is null or coalesce(s.n, 0) <> 1
