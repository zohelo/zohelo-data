{{ config(alias='build_companies_house', tags=['gold_block']) }}
{# Gold build stage: UK Companies House Free Company Data Product, an entity snapshot counted per month
   (silver.companies_house_snapshot_counts: one file per monthly snapshot, kept, so history builds up from the
   first Landing; a month without a landed snapshot has no row). Period = the month the snapshot closes (the
   file dated 1 November is "compiled up to the end of the previous month": period 2026-10, date_key
   2026-10-01); if two files close the same month the later file date is taken. Geography GB: the measures
   describe the UK register; the country of origin is a breakdown (source text as published; only _T and
   POLAND are built), never the area. Measures, monthly, unit companies:
   companies_house:basic_company_data:companies:on_register  companies on the register at the snapshot, with
     country_of_origin (_T, POLAND) and, one at a time, company_status and company_category (others _T);
   companies_house:basic_company_data:companies:incorporated_in_month  companies on the register at the snapshot
     that were incorporated in the period month, UK total only: a lower bound of the month's incorporations
     (a company incorporated and dissolved within the month is not in the file); earlier months are not built
     from a later snapshot (survivorship). Counts exist only where companies do: no zero rows. #}
with counts as (
    select *
    from {{ source('companies_house_silver', 'companies_house_snapshot_counts') }}
    qualify snapshot_date = max(snapshot_date) over (partition by reference_month)
),

scoped as (
    select
        reference_month,
        '_T' as country_of_origin,
        company_category,
        company_status,
        companies,
        incorporated_in_reference_month,
        source_version,
        raw_sha256
    from counts
    union all
    select
        reference_month,
        'POLAND' as country_of_origin,
        company_category,
        company_status,
        companies,
        incorporated_in_reference_month,
        source_version,
        raw_sha256
    from counts
    where country_of_origin = 'POLAND'
),

counted as (  -- each breakdown alone, the other at _T
    {% for cut in ['', 'company_status', 'company_category'] %}
        {% if not loop.first %}union all{% endif %}
        select
            reference_month,
            country_of_origin,
            {{ 'company_category' if cut == 'company_category' else "'_T'" }} as company_category,
            {{ 'company_status' if cut == 'company_status' else "'_T'" }} as company_status,
            sum(companies) as companies,
            sum(incorporated_in_reference_month) as incorporated,
            max(source_version) as source_version,
            max(raw_sha256) as raw_sha256
        from scoped
        group by reference_month, country_of_origin{{ ', ' ~ cut if cut else '' }}
    {% endfor %}
)

select
    'companies_house' as source_system,
    'basic_company_data' as block_id,
    'Companies House Free Company Data Product (monthly snapshot)' as block_name,
    'companies_house:basic_company_data:companies:on_register' as measure_key,
    'Companies on the UK register at the end of the month (Free Company Data Product snapshot)' as measure_name,
    'on_register' as measure_codes,
    'companies' as unit_code,
    'companies' as unit_name,
    'GB' as geography_source_code,
    'GB' as geography_key,
    'M' as frequency,
    reference_month as date_key,
    strftime(reference_month, '%Y-%m') as period_label,
    cast(json_object(
        'company_category', company_category, 'company_status', company_status,
        'country_of_origin', country_of_origin
    ) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    company_category = '_T' and company_status = '_T' and country_of_origin = '_T' as is_total,
    cast(companies as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from counted
union all
select
    'companies_house' as source_system,
    'basic_company_data' as block_id,
    'Companies House Free Company Data Product (monthly snapshot)' as block_name,
    'companies_house:basic_company_data:companies:incorporated_in_month' as measure_key,
    'Companies incorporated in the month and on the UK register at its end (Free Company Data Product snapshot)'
        as measure_name,
    'incorporated_in_month' as measure_codes,
    'companies' as unit_code,
    'companies' as unit_name,
    'GB' as geography_source_code,
    'GB' as geography_key,
    'M' as frequency,
    reference_month as date_key,
    strftime(reference_month, '%Y-%m') as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(incorporated as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from counted
where company_category = '_T' and company_status = '_T' and country_of_origin = '_T'
