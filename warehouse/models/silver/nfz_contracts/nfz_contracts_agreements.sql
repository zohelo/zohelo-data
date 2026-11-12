{# NFZ contracts API (register PLS-014), agreements (bronze.nfz_contracts_agreements): every agreement an NFZ regional
   branch lists for a contract year, from the newest Landing copy of each year and branch (entity
   agreements_<year>_<branch>). Grain: agreement_id (the API's id of the agreement in that contract year; a
   multi-year agreement has one id per year). Values as published (terms: no modification): the amount is the
   agreement's value for the year in PLN as NFZ states it (a contract ceiling, not expenditure), null where NFZ
   publishes none (it is never 0); codes as text with their leading zeros. A text that is not a number with at
   most two decimals (so nothing is rounded) or not a timestamp fails the build. #}
with newest as (
    select *
    from {{ source('nfz_contracts', 'nfz_contracts_agreements') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    agreement_id,
    cast(year_text as integer) as contract_year,
    branch as branch_code,
    code as agreement_code,
    technical_code,
    origin_code,
    service_type as service_type_code,
    service_name as service_type_name,
    case
        when amount_text is null then null
        when regexp_full_match(amount_text, '-?[0-9]+(\.[0-9]{1,2})?') then cast(amount_text as decimal(18, 2))
        else error('nfz_contracts_agreements: amount is not a number with at most 2 decimals: ' || amount_text)
    end as amount_pln,
    coalesce(
        try_cast(updated_at as timestamp),
        error('nfz_contracts_agreements: updated-at is not a timestamp: ' || coalesce(updated_at, 'null'))
    ) as updated_at,
    provider_code,
    provider_nip,
    provider_regon,
    provider_registry_number,
    provider_name,
    provider_place,
    entity,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from newest
