-- mrirw_zsrir (silver.mrirw_zsrir_prices): each of the three markets is in Silver, and every product has a row (a
-- price or a flagged cell) for every month between its first and its last month. A bulletin whose monthly sheet stops
-- matching, or a grid read with a wrong row or column, fails here instead of thinning the series silently. One test
-- per Silver model (a test reading another source's table would fail that source's first runs).
with spans as (
    select
        market,
        product_code,
        min(period_start) as first_month,
        max(period_start) as last_month,
        count(distinct period_start) as months
    from {{ ref('mrirw_zsrir_prices') }}
    group by all
),

markets as (
    select * from (values ('cereals'), ('milk'), ('pork')) as t (market)
)

select
    m.market,
    s.product_code,
    s.first_month,
    s.last_month,
    coalesce(s.months, 0) as months,
    coalesce(date_diff('month', s.first_month, s.last_month) + 1, 1) as expected
from markets as m
left join spans as s on m.market = s.market
where s.market is null or s.months <> date_diff('month', s.first_month, s.last_month) + 1
