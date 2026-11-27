-- GDDKiA rest areas (models/silver/gddkia_mop/gddkia_mop_rest_areas.sql): a column role that stops matching or rows
-- that are not read fail here. Rows returned = problems, per edition:
--   (1) at least 300 rest areas (424 in the September 2026 edition, Dev trial 45829412455);
--   (2) Lp. runs from 1 without gaps (the rows read are all the rows the producer numbered);
--   (3) every row has its branch, locality and rest-area name.
with editions as (
    select
        entity,
        count(*) as rest_areas,
        min(lp) as first_lp,
        max(lp) as last_lp,
        count(*) filter (where branch is null or locality is null or rest_area_name is null) as incomplete
    from {{ ref('gddkia_mop_rest_areas') }}
    group by entity
)

select
    entity,
    'rest areas ' || rest_areas || ', Lp. ' || first_lp || '-' || last_lp || ', incomplete rows ' || incomplete
        as problem
from editions
where rest_areas < 300 or first_lp <> 1 or last_lp <> rest_areas or incomplete > 0
