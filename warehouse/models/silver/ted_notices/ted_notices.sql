{# TED notices of Polish buyers (Search API v3, bronze.ted_notices): one row per published notice, keyed by its
   publication number (<number>-<year>), from the newest Landing copy of each page entity (day_<YYYYMMDD>_p<n>).
   Every field is read as a list (the API answers a list, a single value or a language map); a field that is one
   value per notice (types, dates, procedure, estimated value and its currency) fails the build when a notice has
   more than one value, instead of taking one. Codes as published (eForms code lists): notice type, form type,
   contract nature, procedure type, CPV, ISO alpha-3 buyer countries, buyer NUTS codes, buyer legal type. Estimated
   value of the procedure (BT-27) as published, with its currency (never converted or summed here). Buyer names are
   organisation names as published (all languages given). A notice found on two pages fails the unique test. #}
{% set listed = [
    'notice_type', 'form_type', 'publication_date', 'dispatch_date', 'contract_nature', 'procedure_type',
    'classification_cpv', 'buyer_country', 'buyer_country_sub', 'buyer_legal_type', 'estimated_value_proc',
    'estimated_value_cur_proc'
] %}
{% set single = [
    'notice_type', 'form_type', 'publication_date', 'dispatch_date', 'procedure_type', 'estimated_value_proc',
    'estimated_value_cur_proc'
] %}
with newest as (
    select *
    from {{ source('ted_notices', 'ted_notices') }}
    qualify rank() over (partition by entity order by ingested_at desc) = 1
),

lists as (
    select
        entity,
        publication_number,
        {% for c in listed %}
            case
                when {{ c }} is null then cast(null as varchar [])
                when left({{ c }}, 1) = '[' then list_distinct(json_extract_string({{ c }}, '$[*]'))
                when left({{ c }}, 1) = '{' then list_distinct(json_extract_string({{ c }}, '$.*'))
                else [{{ c }}]
            end as {{ c }},
        {% endfor %}
        case
            when buyer_name is null then cast(null as varchar [])
            when left(buyer_name, 1) = '{' then list_distinct(flatten(list_transform(
                json_extract(buyer_name, '$.*'),
                lambda x: case
                    when json_type(x) = 'ARRAY' then json_extract_string(x, '$[*]')
                    else [json_extract_string(x, '$')]
                end
            )))
            when left(buyer_name, 1) = '[' then list_distinct(json_extract_string(buyer_name, '$[*]'))
            else [buyer_name]
        end as buyer_names,
        raw_sha256,
        source_version,
        ingested_at,
        source_row_number
    from newest
),

checked as (
    select
        *,
        {% for c in single %}
            case
                when len({{ c }}) > 1
                    then error('ted_notices: ' || publication_number || ' has several {{ c }} ' || {{ c }}::varchar)
                else {{ c }}[1]
            end as {{ c }}_one{{ ',' if not loop.last }}
        {% endfor %}
    from lists
)

select
    publication_number,
    cast(split_part(publication_number, '-', 2) as integer) as publication_year,
    'https://ted.europa.eu/en/notice/-/detail/' || publication_number as notice_url,
    notice_type_one as notice_type,
    form_type_one as form_type,
    cast(left(publication_date_one, 10) as date) as publication_date,
    cast(left(dispatch_date_one, 10) as date) as dispatch_date,
    procedure_type_one as procedure_type,
    list_sort(contract_nature) as contract_nature,
    list_sort(classification_cpv) as cpv_codes,
    list_sort(buyer_country) as buyer_countries,
    list_sort(buyer_country_sub) as buyer_nuts,
    list_sort(buyer_legal_type) as buyer_legal_types,
    list_sort(buyer_names) as buyer_names,
    cast(estimated_value_proc_one as double) as estimated_value,
    estimated_value_cur_proc_one as estimated_value_currency,
    entity,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from checked
