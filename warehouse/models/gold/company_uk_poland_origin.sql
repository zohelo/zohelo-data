{{ config(alias='company_uk_poland_origin') }}
{# Gold entity table (register source, playbook section 7): the companies on the UK register whose country of
   origin is Poland, newest Companies House snapshot, one row per company number. Key company_key = 'GB-COH:' ||
   company number (the register's own id; the file has no KRS, NIP, REGON or LEI). A join to GLEIF is made on the
   GLEIF side, where an LEI record names its registration authority and the entity's id there; nothing is matched
   by name. origin_geography_key conforms the country of origin (source text POLAND) to dim_geography. SIC codes
   are the code part of the file's "<code> - <description>" texts ('None Supplied' → null). Registered office:
   post town and country only, free text as published. #}
select
    'GB-COH:' || company_number as company_key,
    company_number,
    company_name,
    company_category,
    company_status,
    country_of_origin,
    'PL' as origin_geography_key,
    incorporation_date,
    {% for n in [1, 2, 3, 4] %}
        case when regexp_matches(sic_text_{{ n }}, '^[0-9]{4,5} - ') then split_part(sic_text_{{ n }}, ' - ', 1) end
            as sic_code_{{ n }},
    {% endfor %}
    post_town,
    address_country,
    snapshot_date,
    source_version,
    raw_sha256
from {{ source('companies_house_silver', 'companies_house_poland_origin') }}
