-- knf_banking: every workbook in Bronze yields Silver rows. A workbook whose date header rows the Silver model no
-- longer finds would otherwise leave the source empty silently; returns the entity without Silver rows.
with bronze_entities as (
    select distinct entity
    from {{ source('knf_banking', 'knf_banking_cells') }}
),

silver_entities as (
    select distinct entity
    from {{ ref('knf_banking_series') }}
)

select b.entity
from bronze_entities as b
left join silver_entities as s on b.entity = s.entity
where s.entity is null
