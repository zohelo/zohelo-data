-- NFZ contracts (register PLS-014): silver.nfz_contracts_providers has every branch and provider code of
-- silver.nfz_contracts_agreements with the same number of agreements and contract years, and its presence columns
-- agree with each other. Returns the provider that differs.
with expected as (
    select
        branch_code,
        provider_code,
        count(*) as agreements,
        count(distinct contract_year) as contract_years
    from {{ ref('nfz_contracts_agreements') }}
    group by branch_code, provider_code
)

select
    coalesce(e.branch_code, p.branch_code) as branch_code,
    coalesce(e.provider_code, p.provider_code) as provider_code,
    e.agreements as expected_agreements,
    p.agreements
from expected as e
full outer join {{ ref('nfz_contracts_providers') }} as p
    on e.branch_code = p.branch_code and e.provider_code = p.provider_code
where
    p.provider_code is null
    or e.provider_code is null
    or e.agreements <> p.agreements
    or e.contract_years <> p.contract_years
    or p.first_contract_year < {{ poc_nfz_contracts_first_year() }}
    or p.first_contract_year > p.last_contract_year
    or p.contract_years > p.last_contract_year - p.first_contract_year + 1
