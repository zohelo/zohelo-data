-- gddkia_gpr: every file in Bronze (entity gpr<edition>_<dk|dw>_<table>[_errata]) yields Silver rows. A file whose
-- header texts the Silver model no longer finds would otherwise leave its table out silently; returns the entity
-- without Silver rows.
with bronze_entities as (
    select distinct entity
    from {{ source('gddkia_gpr', 'gddkia_gpr_cells') }}
),

silver_entities as (
    select distinct entity
    from {{ ref('gddkia_gpr_sdrr') }}
)

select b.entity
from bronze_entities as b
left join silver_entities as s on b.entity = s.entity
where s.entity is null
