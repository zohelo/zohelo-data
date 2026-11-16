-- ISWS multi-year workbooks (warehouse/models/silver/ms_isws/ms_isws_tables.sql): every year from the table's
-- first year to the newest year of any table has every measure x breakdown of that table: caseload 21 (3 measures
-- x total and 6 case types, from 2011), duration 3 (total, regional, district, from 2011), adoptions 4 (from 2000),
-- notarial acts 1 (from 1991). A label, header or sheet that stops matching in a new edition fails here.
with expected (table_code, first_year, n) as (
    values ('caseload', 2011, 21), ('duration', 2011, 3), ('adoptions', 2000, 4), ('notarial_acts', 1991, 1)
),

counts as (
    select
        table_code,
        period_year,
        count(*) as n
    from {{ ref('ms_isws_tables') }}
    group by all
),

grid as (
    select
        e.table_code,
        e.n,
        unnest(generate_series(e.first_year, (select max(c.period_year) from counts as c))) as period_year
    from expected as e
)

select
    g.table_code,
    g.period_year,
    coalesce(c.n, 0) as found,
    g.n as expected
from grid as g
left join counts as c on g.table_code = c.table_code and g.period_year = c.period_year
where coalesce(c.n, 0) <> g.n
