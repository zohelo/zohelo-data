-- NFZ contracts (register PLS-014): every contract year from FIRST_YEAR (2019, ingestion/to_landing/nfz_contracts/
-- extract.py) to the newest landed year has agreements of each of the 16 NFZ regional branches. A year or branch
-- missing from Landing, or rejected by Bronze, returns a row here.
with counts as (
    select
        contract_year,
        branch_code,
        count(*) as agreements
    from {{ ref('nfz_contracts_agreements') }}
    group by all
),

grid as (
    select
        y.contract_year,
        lpad(cast(b.n as varchar), 2, '0') as branch_code
    from (select unnest(generate_series(2019, max(contract_year))) as contract_year from counts) as y
    cross join (select unnest(generate_series(1, 16)) as n) as b
)

select
    g.contract_year,
    g.branch_code,
    coalesce(c.agreements, 0) as agreements
from grid as g
left join counts as c on g.contract_year = c.contract_year and g.branch_code = c.branch_code
where c.agreements is null
