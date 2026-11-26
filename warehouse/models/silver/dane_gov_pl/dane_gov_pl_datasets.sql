{# dane.gov.pl open-data catalogue as harvested by data.europa.eu (register PLG-030, #3263): one row per dataset of
   dane.gov.pl, from the newest Landing copy (bronze.dane_gov_pl_datasets, distributions counted from
   bronze.dane_gov_pl_distributions). Grain: dataset_number, the dane.gov.pl dataset number (from the identifier
   https://dane.gov.pl/pl/dataset/<n>,<slug>, else from the data.europa.eu id https-dane-gov-pl-pl-dataset-<n>-...).
   DEDUPE_PLACEHOLDER
   Typed: times (ISO 8601 with offset, UTC), the high-value flag; codes as published at the end of their EU vocabulary
   URI (periodicity: DAILY, ANNUAL, ...); publisher_number from the dane.gov.pl institution link
   (https://dane.gov.pl/pl/institution/<n>,<slug>). Not here: descriptions, keywords and contacts (never landed). #}
with newest as (
    select *
    from {{ source('dane_gov_pl', 'dane_gov_pl_datasets') }}
    qualify rank() over (order by source_version desc, ingested_at desc) = 1
),

distributions as (
    select
        dataset_id,
        count(*) as distribution_count
    from {{ source('dane_gov_pl', 'dane_gov_pl_distributions') }}
    where raw_sha256 = (select max(raw_sha256) from newest)
    group by dataset_id
),

records as (
    select
        cast(coalesce(
            regexp_extract(identifier, 'dane\.gov\.pl/(?:[a-z]{2}/)?dataset/([0-9]+)', 1),
            regexp_extract(dataset_id, '^https-dane-gov-pl-(?:[a-z]{2}-)?dataset-([0-9]+)-', 1)
        ) as bigint) as dataset_number,
        n.dataset_id,
        n.identifier as identifier_url,
        n.landing_page,
        nullif(trim(n.title_pl), '') as title_pl,
        try_cast(regexp_extract(n.publisher_resource, '/institution/([0-9]+)', 1) as bigint) as publisher_number,
        nullif(trim(n.publisher_name), '') as publisher_name,
        n.publisher_type,
        n.publisher_resource as publisher_url,
        regexp_extract(n.accrual_periodicity, '/frequency/([A-Z0-9_]+)$', 1) as periodicity_code,
        cast(json_array_length(n.categories) as integer) as category_count,
        case n.is_hvd when 'true' then true when 'false' then false end as is_high_value,
        cast(n.issued as timestamptz) as issued_at,
        cast(n.modified as timestamptz) as modified_at,
        cast(n.catalog_record_issued as timestamptz) as harvest_issued_at,
        cast(n.catalog_record_modified as timestamptz) as harvest_modified_at,
        n.country as country_code,
        coalesce(d.distribution_count, 0) as distribution_count,
        n.source_row_number,
        n.source_version,
        n.raw_sha256
    from newest as n
    left join distributions as d on n.dataset_id = d.dataset_id
)

select
    *,
    cast(timezone('UTC', modified_at) as date) as modified_on
from records
