{# NFZ contracts API (register PLS-014), healthcare providers: the entity table of the providers that hold an NFZ
   agreement in the landed contract years, from silver.nfz_contracts_agreements (the API repeats the provider's
   identifiers on every agreement). Grain: branch_code × provider_code (NFZ gives each provider a code per regional
   branch, so one legal entity contracting with two branches has two rows; NIP and REGON are kept for linking, never
   the name). Attributes as published on the provider's most recently updated agreements (newest contract year, then
   newest updated-at: the newest version of the entity); identical copies of them are one row, and when such
   agreements disagree on an attribute the rows differ and the unique test fails instead of one being picked.
   first_contract_year, last_contract_year and contract_years describe its presence. #}
with agreements as (
    select * from {{ ref('nfz_contracts_agreements') }}
),

presence as (
    select
        branch_code,
        provider_code,
        min(contract_year) as first_contract_year,
        max(contract_year) as last_contract_year,
        count(distinct contract_year) as contract_years
    from agreements
    group by branch_code, provider_code
),

newest as (
    select *
    from agreements
    qualify rank() over (partition by branch_code, provider_code order by contract_year desc, updated_at desc) = 1
)

select distinct
    a.branch_code,
    a.provider_code,
    a.provider_nip,
    a.provider_regon,
    a.provider_registry_number,
    a.provider_name,
    a.provider_place,
    p.first_contract_year,
    p.last_contract_year,
    p.contract_years
from newest as a
inner join presence as p on a.branch_code = p.branch_code and a.provider_code = p.provider_code
