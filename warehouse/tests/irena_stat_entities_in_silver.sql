-- irena_stat: every entity in Bronze yields Silver rows. A table whose layout the Silver model no longer reads
-- would otherwise drop out silently; returns the entity without Silver rows.
with bronze_entities as (
    select distinct entity
    from {{ source('irena_stat', 'irena_stat_electricity') }}
),

silver_entities as (
    select distinct entity
    from {{ ref('irena_stat_electricity') }}
)

select b.entity
from bronze_entities as b
left join silver_entities as s on b.entity = s.entity
where s.entity is null
