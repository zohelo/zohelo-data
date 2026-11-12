{# NFZ statistics API, services (JGP): the benefit dictionary of the five catalogs (1a JGP groups, 1b separate
   benefits, 1c benefits added up, 1d radiotherapy, 1w highly specialised), newest copy of each catalog.
   Grain: catalog × benefit_code (NFZ product code). Source: bronze.nfz_services_benefits (entity
   benefits_<catalog>). A code listed twice in a catalog fails the unique test. #}
with newest as (
    select *
    from {{ source('nfz_services', 'nfz_services_benefits') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    catalog,
    benefit_code,
    benefit_name,
    source_row_number,
    raw_sha256,
    source_version,
    ingested_at
from newest
