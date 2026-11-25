-- nid_monuments (register PLS-026): in every list, at most 10 % of the numbered items have no decision with a full
-- date (a register number printed without " z <dd.mm.yyyy>", a year or a two-digit year only, a typo). A change in
-- how a list prints its decisions fails here instead of turning many entries into number-only keys. One test per
-- Silver model, reading only that model.
with per_item as (
    select
        list_code,
        item_number,
        bool_or(decision_date is not null) as dated
    from {{ ref('nid_monuments_decisions') }}
    where register_role = 'numbered'
    group by list_code, item_number
)

select
    list_code,
    count(*) as numbered_items,
    count(*) filter (where not dated) as undated
from per_item
group by list_code
having count(*) filter (where not dated) > 0.1 * count(*)
