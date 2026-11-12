{{ config(alias='build_nfz_contracts', tags=['gold_block']) }}
{# Gold build stage: NFZ contracts API (register PLS-014), counts and sums over silver.nfz_contracts_agreements, by
   contract year (frequency A), NFZ regional branch (= voivodeship, poc_geography__nfz_contracts) and service type.
   These are zohelo calculations over the published register, not values NFZ publishes: agreements (rows of the
   register), providers (distinct NFZ provider codes; a code belongs to one branch, so Poland is the sum of the
   branches and a provider contracting with two branches counts twice) and contracted amount (sum of the published
   agreement amounts in PLN; agreements without an amount are left out of the sum, never counted as 0; a contract
   ceiling for the year, not expenditure). Poland ('all_branches') is the sum over the 16 branches. Breakdown:
   service type (NFZ code as published) and the total over the types (is_total). #}
with agreements as (
    select
        contract_year,
        branch_code,
        service_type_code,
        service_type_name,
        provider_code,
        amount_pln,
        source_version,
        raw_sha256
    from {{ ref('nfz_contracts_agreements') }}
),

areas as (
    select
        branch_code as area_code,
        *
    from agreements
    union all
    select
        'all_branches' as area_code,
        *
    from agreements
),

by_type as (
    select
        area_code,
        contract_year,
        service_type_code,
        string_agg(distinct service_type_name, ' / ' order by service_type_name) as service_type_name,
        count(*) as agreements,
        count(distinct branch_code || ':' || provider_code) as providers,
        sum(amount_pln) as contracted_amount,
        max(source_version) as source_version,
        arg_max(raw_sha256, source_version || raw_sha256) as raw_sha256  -- the newest Landing file counted
    from areas
    group by area_code, contract_year, service_type_code
),

all_types as (
    select
        area_code,
        contract_year,
        '_T' as service_type_code,
        cast(null as varchar) as service_type_name,
        count(*) as agreements,
        count(distinct branch_code || ':' || provider_code) as providers,
        sum(amount_pln) as contracted_amount,
        max(source_version) as source_version,
        arg_max(raw_sha256, source_version || raw_sha256) as raw_sha256
    from areas
    group by area_code, contract_year
),

counted as (
    select * from by_type
    union all
    select * from all_types
),

measures as (
    select
        *,
        'agreements' as measure_code,
        'agreements' as unit_code,
        cast(agreements as double) as value
    from counted
    union all
    select
        *,
        'providers' as measure_code,
        'healthcare_providers' as unit_code,
        cast(providers as double) as value
    from counted
    union all
    select
        *,
        'contracted_amount' as measure_code,
        'pln' as unit_code,
        cast(contracted_amount as double) as value
    from counted
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'nfz_contracts'
)

select
    'nfz_contracts' as source_system,
    'agreements' as block_id,
    'NFZ contracts API: agreements by contract year and branch' as block_name,
    'nfz_contracts:agreements:' || m.unit_code || ':' || m.measure_code as measure_key,
    case m.measure_code
        when 'agreements' then 'NFZ agreements in the contract year (count, zohelo calculation)'
        when 'providers'
            then 'Healthcare providers with an NFZ agreement in the contract year (NFZ provider codes, zohelo count)'
        when 'contracted_amount'
            then 'Contracted amount of NFZ agreements in the contract year (PLN, zohelo sum of published amounts)'
    end as measure_name,
    m.measure_code as measure_codes,
    m.unit_code,
    case m.unit_code
        when 'agreements' then 'agreements'
        when 'healthcare_providers' then 'healthcare providers'
        when 'pln' then 'PLN'
    end as unit_name,
    m.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(m.contract_year, 1, 1) as date_key,
    cast(m.contract_year as varchar) as period_label,
    case
        when m.service_type_code = '_T' then '{}'
        else '{"service_type":"' || m.service_type_code || '"}'
    end as breakdown_json,
    case
        when m.service_type_code = '_T' then cast(null as varchar)
        else cast(json_object('service_type', m.service_type_name) as varchar)
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    m.service_type_code = '_T' as is_total,
    m.value,
    'ok' as observation_status_key,
    m.source_version,
    m.raw_sha256
from measures as m
left join geo as g on m.area_code = g.source_code
where m.value is not null
