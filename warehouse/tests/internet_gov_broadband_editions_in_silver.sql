-- internet_gov_broadband (register PL-ENV-034): every table of every edition in the newest Bronze copy yields Silver
-- rows, so no landed edition or table is lost between the layers (a label or reference date the Silver model no
-- longer reads would otherwise drop it silently). Returns each entity x table in Bronze and not in Silver.
with bronze as (
    select distinct
        entity,
        table_code
    from {{ source('internet_gov_broadband', 'internet_gov_broadband_cells') }}
    qualify dense_rank() over (partition by entity order by ingested_at desc, source_version desc) = 1
)

select
    b.entity,
    b.table_code
from bronze as b
left join (select distinct entity, table_code from {{ ref('internet_gov_broadband_coverage') }}) as s
    on b.entity = s.entity and b.table_code = s.table_code
where s.entity is null
