-- gddkia_mop: every edition in Bronze yields Silver rows. An edition whose header texts the Silver model no longer
-- finds would otherwise leave its month out silently; returns the edition without Silver rows.
with bronze_entities as (
    select distinct entity
    from {{ source('gddkia_mop', 'gddkia_mop_cells') }}
),

silver_entities as (
    select distinct entity
    from {{ ref('gddkia_mop_rest_areas') }}
)

select b.entity
from bronze_entities as b
left join silver_entities as s on b.entity = s.entity
where s.entity is null
