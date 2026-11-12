-- UNECE (#2861, #2885): gold.build_unece joins silver.unece_observations to macro poc_unece_measures on the exact
-- PXWeb value codes of each built table. Returns the listed combinations that match no row in Silver (the UNECE
-- renamed a code or a table: the measure would lose its rows) and the combinations a built table carries that
-- are not listed (a new indicator would be left out without notice).
with measures as ({{ poc_unece_measures() }}),

blocks as ({{ poc_unece_blocks() }}),

series as (
    select
        o.table_id,
        {{ poc_unece_codes() | trim }} as codes,
        count(*) as silver_rows
    from {{ ref('unece_observations') }} as o
    inner join blocks as b on o.table_id = b.table_id
    where o.obs_value is not null
    group by all
)

select
    coalesce(m.table_id, s.table_id) as table_id,
    coalesce(m.codes, s.codes) as codes,
    case when s.table_id is null then 'listed, no rows in Silver' else 'in Silver, not listed' end as problem,
    s.silver_rows
from measures as m
full outer join series as s on m.table_id = s.table_id and m.codes = s.codes
where m.table_id is null or s.table_id is null
