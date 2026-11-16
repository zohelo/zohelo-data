{# NFZ contracts API (register PLS-014), healthcare providers: the providers that hold an NFZ agreement in the landed
   contract years, from silver.nfz_contracts_agreements. Grain: branch_code × provider_code (NFZ gives each provider a
   code per regional branch, so one legal entity contracting with two branches has two rows). Only the code and its
   presence (first_contract_year, last_contract_year, contract_years, agreements): the provider's name, NIP, REGON,
   registry number and place are not loaded, because providers include natural persons (individual and group
   practices) and the register entry gives no basis for storing their names or identifiers (owner GDPR review before
   any person-level load). #}
select
    branch_code,
    provider_code,
    min(contract_year) as first_contract_year,
    max(contract_year) as last_contract_year,
    count(distinct contract_year) as contract_years,
    count(*) as agreements
from {{ ref('nfz_contracts_agreements') }}
group by branch_code, provider_code
