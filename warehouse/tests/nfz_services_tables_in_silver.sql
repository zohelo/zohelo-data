-- NFZ services: every basic-data table with records in the newest Bronze copy of a benefit has its rows in
-- silver.nfz_services_basic_data (Poland and each branch record), so no landed table is lost between the layers.
-- Returns each entity × table × area that is in Bronze and not in Silver.
with bronze as (
    select
        entity,
        table_id,
        coalesce(branch, 'PL') as area_code
    from {{ source('nfz_services', 'nfz_services_basic_data') }}
    where record = 'value'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    b.entity,
    b.table_id,
    b.area_code
from bronze as b
left join {{ ref('nfz_services_basic_data') }} as s
    on b.entity = s.entity and b.table_id = s.table_id and b.area_code = s.area_code
where s.entity is null
