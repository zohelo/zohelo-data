-- gpw_ebi: every day held in Bronze is in Silver with all its reports (the newest copy of the day), and every
-- report sits in the day document of its own day of publication. The GPW list states no count per day, so this is
-- the completeness that can be checked: no held day or report lost between Bronze and Silver, no misfiled row.
-- One test per Silver model.
with bronze_copies as (
    select
        entity,
        ingested_at,
        count(*) as rows_newest
    from {{ source('gpw_ebi', 'gpw_ebi_reports') }}
    group by entity, ingested_at
),

bronze_days as (
    select
        entity,
        rows_newest
    from bronze_copies
    qualify rank() over (partition by entity order by ingested_at desc) = 1
),

silver_days as (
    select
        entity,
        count(*) as rows_silver,
        count(*) filter (where entity <> 'day_' || strftime(published_date, '%Y%m%d')) as misfiled
    from {{ ref('gpw_ebi_reports') }}
    group by entity
)

select
    coalesce(b.entity, s.entity) as entity,
    b.rows_newest,
    s.rows_silver,
    s.misfiled
from bronze_days as b
full outer join silver_days as s on b.entity = s.entity
where b.rows_newest is distinct from s.rows_silver or s.misfiled > 0
