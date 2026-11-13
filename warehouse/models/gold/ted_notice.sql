{{ config(alias='ted_notice') }}
{# Gold entity table (register source, playbook section 7): one row per TED notice of Polish buyers, keyed by the
   TED publication number (<number>-<year>), from silver.ted_notices. The publication day is the date key
   (dim_calendar); buyer areas are the voivodeship keys ('TERC:<code>', dim_geography) of the notice's Polish buyers,
   from their NUTS codes through map_geography (the same rule as gold.build_ted_notices; '_X' for a code that maps to
   no voivodeship, an empty list when no Polish buyer gives a regional code). Codes and the estimated value with its
   currency as published; no person data (the source rows hold organisation names only). #}
with notices as (
    select * from {{ ref('ted_notices') }}
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'ted_notices'
),

codes as (
    select
        n.publication_number,
        case when u.code like 'PL9%' then 'PL9' else left(u.code, 4) end as geo_code
    from notices as n, unnest(n.buyer_nuts) as u (code)
    where u.code like 'PL%' and u.code not like 'PLZ%'
),

regions as (
    select
        c.publication_number,
        list_sort(list_distinct(list(coalesce(g.geography_key, '_X')))) as buyer_geography_keys
    from codes as c
    left join geo_map as g on c.geo_code = g.source_code
    group by c.publication_number
)

select
    n.publication_number,
    n.publication_year,
    n.notice_url,
    n.publication_date as date_key,
    n.dispatch_date,
    n.notice_type,
    n.form_type,
    n.procedure_type,
    n.contract_nature,
    n.cpv_codes,
    n.buyer_names,
    n.buyer_countries,
    n.buyer_nuts,
    n.buyer_legal_types,
    coalesce(r.buyer_geography_keys, cast([] as varchar [])) as buyer_geography_keys,
    n.estimated_value,
    n.estimated_value_currency,
    n.source_version,
    n.raw_sha256
from notices as n
left join regions as r on n.publication_number = r.publication_number
