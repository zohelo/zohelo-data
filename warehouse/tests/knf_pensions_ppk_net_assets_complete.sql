-- knf_pensions: the total row ('Suma') has exactly one value for every quarter from the first to the last, and every
-- manager row has the same quarters as the total. A year header that moves (the years are carried from merged cells)
-- or a lost column fails here instead of shifting values to another quarter. One test per Silver model.
with quarters as (
    select
        period_year * 4 + period_quarter - 1 as q,
        count(*) filter (where is_total) as totals,
        count(*) filter (where cell_status = 'ok' and is_total) as total_values
    from {{ ref('knf_pensions_ppk_net_assets') }}
    group by all
),

span as (
    select unnest(range(min(q), max(q) + 1)) as q
    from quarters
),

per_manager as (
    select
        manager_label,
        count(*) as quarters_found
    from {{ ref('knf_pensions_ppk_net_assets') }}
    group by all
)

select
    'quarter ' || s.q // 4 || '-Q' || s.q % 4 + 1 as problem,
    coalesce(x.totals, 0) as found
from span as s
left join quarters as x on s.q = x.q
where coalesce(x.totals, 0) <> 1 or coalesce(x.total_values, 0) <> 1
union all
select
    'manager ' || manager_label as problem,
    quarters_found as found
from per_manager
where quarters_found <> (select count(*) from span)
