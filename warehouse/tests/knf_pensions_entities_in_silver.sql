-- knf_pensions: every workbook in Bronze yields Silver rows. A release whose 'Tabela 2' header the Silver model no
-- longer finds would otherwise leave the source empty silently; returns the entity without Silver rows.
with bronze_entities as (
    select distinct entity
    from {{ source('knf_pensions', 'knf_pensions_cells') }}
),

silver_entities as (
    select distinct entity
    from {{ ref('knf_pensions_ppk_net_assets') }}
)

select b.entity
from bronze_entities as b
left join silver_entities as s on b.entity = s.entity
where s.entity is null
