-- world_bank_es: every entity in Bronze yields Silver rows. A file whose layout the Silver model no longer reads would
-- otherwise drop out silently; returns the entity without Silver rows.
with bronze_entities as (
    select distinct entity
    from {{ source('world_bank_es', 'world_bank_es_observations') }}
),

silver_entities as (
    select distinct entity
    from {{ ref('world_bank_es_observations') }}
)

select b.entity
from bronze_entities as b
left join silver_entities as s on b.entity = s.entity
where s.entity is null
