-- UPRP industrial-property statistics (silver.uprp_statistics_counts): each of the nine tables has its total
-- ("Suma końcowa") for every year from the first year it has to the newest year of the model, with no gap. A table
-- whose title, header or rows stop being read in a new report, or whose cells stop passing their sums, leaves a gap
-- (or disappears) and fails here. Returns the table x year cells without a total.
with totals as (
    select
        table_code,
        year
    from {{ ref('uprp_statistics_counts') }}
    where row_code = 'total'
),

spans as (
    select
        t.table_code,
        min(t.year) as first_year,
        (select max(a.year) from totals as a) as last_year
    from totals as t
    group by t.table_code
),

expected as (
    select
        s.table_code,
        unnest(generate_series(s.first_year, s.last_year)) as year
    from spans as s
    union all
    select
        w.table_code,
        null as year
    from (
        values
        ('inventions_applications'), ('inventions_grants'), ('utility_models_applications'),
        ('utility_models_grants'), ('european_patents_validations'), ('trademarks_applications'),
        ('trademarks_grants'), ('industrial_designs_applications'), ('industrial_designs_grants')
    ) as w (table_code)
    where w.table_code not in (select s.table_code from spans as s)
)

select
    e.table_code,
    e.year
from expected as e
left join totals as t on e.table_code = t.table_code and e.year = t.year
where t.table_code is null
