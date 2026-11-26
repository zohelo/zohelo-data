-- cepik: every workbook in Bronze (entity vehicles_voivodeship_<year> / vehicles_type_<year>) yields Silver rows. A
-- workbook whose header labels or month names the Silver model no longer finds would otherwise leave its year out
-- silently; returns the entity without Silver rows.
with bronze_entities as (
    select distinct entity
    from {{ source('cepik', 'cepik_cells') }}
),

silver_entities as (
    select distinct entity
    from {{ ref('cepik_vehicle_registrations') }}
)

select b.entity
from bronze_entities as b
left join silver_entities as s on b.entity = s.entity
where s.entity is null
