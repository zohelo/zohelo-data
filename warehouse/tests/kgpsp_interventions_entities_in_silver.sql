-- kgpsp_interventions: every year in Bronze (entity tabele_<year>) yields Silver rows of both tables. A workbook whose
-- header texts the Silver model no longer finds would otherwise leave its year out silently; returns the entity and
-- the member without Silver rows.
with bronze_members as (
    select distinct
        entity,
        member
    from {{ source('kgpsp_interventions', 'kgpsp_interventions_cells') }}
),

silver_members as (
    select distinct
        entity,
        member
    from {{ ref('kgpsp_interventions_counts') }}
)

select
    b.entity,
    b.member
from bronze_members as b
left join silver_members as s on b.entity = s.entity and b.member = s.member
where s.entity is null
