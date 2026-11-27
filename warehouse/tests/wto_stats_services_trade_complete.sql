-- wto_stats_services_trade: every year from the first to the last has Poland's total services exports and imports
-- with the World (reporter PL, partner WL, item S, flows X and M), and every partner x flow x year that Poland reports
-- carries the total 'S' and all twelve EBOPS 2010 main items SA … SL (BaTIS is a complete matrix). A selection or a
-- file that loses rows, or a release that renames an item, fails here. One test per Silver model.
with t as (select * from {{ ref('wto_stats_services_trade') }}),

years as (
    select unnest(generate_series(min(year), max(year))) as year
    from t
),

flows as (select * from (values ('X'), ('M')) as f (flow)),

missing_world as (
    select
        y.year,
        f.flow,
        'no PL-WL total' as problem,
        cast(null as varchar) as partner_code,
        cast(null as bigint) as main_items
    from years as y
    cross join flows as f
    where not exists (
        select 1 from t
        where
            t.year = y.year and t.flow = f.flow and t.reporter_code = 'PL'
            and t.partner_code = 'WL' and t.item_code = 'S'
    )
),

main_items as (
    select
        year,
        flow,
        'main items incomplete' as problem,
        partner_code,
        count(distinct item_code) as main_items
    from t
    where reporter_code = 'PL' and item_code in ('S', 'SA', 'SB', 'SC', 'SD', 'SE', 'SF', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL')
    group by year, flow, partner_code
    having count(distinct item_code) <> 13
)

select * from missing_world
union all
select * from main_items
