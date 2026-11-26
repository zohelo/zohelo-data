-- nid_monuments (register PLS-026): every one of the 17 register lists is in Silver with at least 1,000 items (the
-- smallest list, podlaskie, had 2,424 on 30.06.2026, Dev probe 43799285327). A list whose page layout stops matching
-- the decoder is rejected in Bronze and shows up here instead of thinning the counts silently. One test per Silver
-- model, reading only that model.
with lists (list_code) as (
    values
    ('dln'), ('kuj'), ('lbl'), ('lbs'), ('ldz'), ('mal'), ('maz'), ('maz_war'), ('opo'), ('pdk'), ('pdl'), ('pom'),
    ('sls'), ('swt'), ('war'), ('wlk'), ('zpo')
),

found as (
    select
        list_code,
        count(*) as items
    from {{ ref('nid_monuments_items') }}
    group by list_code
)

select
    l.list_code,
    coalesce(f.items, 0) as items
from lists as l
left join found as f on l.list_code = f.list_code
where coalesce(f.items, 0) < 1000
