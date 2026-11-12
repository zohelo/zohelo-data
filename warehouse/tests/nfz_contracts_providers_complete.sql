-- NFZ contracts (register PLS-014): the provider entity table covers each of the 16 NFZ regional branches in every
-- contract year from FIRST_YEAR (2019) to the newest year it holds (a provider counts for the years between its first
-- and last contract year), and its presence columns are consistent. Returns the branch and year without a provider,
-- or the provider whose presence columns contradict each other.
with spans as (
    select * from {{ ref('nfz_contracts_providers') }}
),

grid as (
    select
        y.contract_year,
        lpad(cast(b.n as varchar), 2, '0') as branch_code
    from (select unnest(generate_series(2019, max(last_contract_year))) as contract_year from spans) as y
    cross join (select unnest(generate_series(1, 16)) as n) as b
)

select
    g.branch_code,
    g.contract_year,
    'no provider' as problem
from grid as g
where not exists (
    select 1
    from spans as s
    where s.branch_code = g.branch_code and g.contract_year between s.first_contract_year and s.last_contract_year
)
union all
select
    branch_code,
    first_contract_year,
    'presence columns contradict: provider ' || provider_code as problem
from spans
where first_contract_year < 2019
    or first_contract_year > last_contract_year
    or contract_years not between 1 and last_contract_year - first_contract_year + 1
