{# dane.gov.pl open-data catalogue as harvested by data.europa.eu (register PLG-030, #3263): one row per dataset of
   dane.gov.pl, from the newest Landing copy (bronze.dane_gov_pl_datasets, distributions counted from
   bronze.dane_gov_pl_distributions). Grain: dataset_number, the dane.gov.pl dataset number (from the identifier
   https://dane.gov.pl/pl/dataset/<n>,<slug>, else from the data.europa.eu id https-dane-gov-pl-pl-dataset-<n>-...).
   Versions: data.europa.eu derives its id from the dataset's URL, so when a dataset's slug changes on dane.gov.pl the
   harvest adds a record and keeps the old one (Dev probe 45818878323: 31 numbers on two records, each pair with the
   same publisher and issued time). Those records are versions of one dataset: the newest by modified (the dataset's
   last modification on dane.gov.pl), then by the harvest record's modified, is kept; harvest_records counts them.
   Two records that tie on both fail the build (no choice by position). Typed: times (ISO 8601, UTC), the high-value
   flag; periodicity_code as published at the end of its EU vocabulary URI (DAILY, ANNUAL, ...); publisher_number from
   the dane.gov.pl institution link (https://dane.gov.pl/pl/institution/<n>,<slug>). Not here: descriptions, keywords
   and contacts (never landed). #}
with newest as (
    select *
    from {{ source('dane_gov_pl', 'dane_gov_pl_datasets') }}
    qualify rank() over (order by source_version desc, ingested_at desc) = 1
),

distributions as (
    select
        b.dataset_id,
        count(*) as distribution_count
    from {{ source('dane_gov_pl', 'dane_gov_pl_distributions') }} as b
    semi join newest as n on b.raw_sha256 = n.raw_sha256
    group by b.dataset_id
),

records as (
    select
        cast(coalesce(
            nullif(regexp_extract(n.identifier, 'dane\.gov\.pl/(?:[a-z]{2}/)?dataset/([0-9]+)', 1), ''),
            nullif(regexp_extract(n.dataset_id, '^https-dane-gov-pl-(?:[a-z]{2}-)?dataset-([0-9]+)-', 1), '')
        ) as bigint) as dataset_number,
        n.dataset_id,
        n.identifier as identifier_url,
        n.landing_page,
        nullif(trim(n.title_pl), '') as title_pl,
        try_cast(nullif(regexp_extract(n.publisher_resource, '/institution/([0-9]+)', 1), '') as bigint)
            as publisher_number,
        nullif(trim(n.publisher_name), '') as publisher_name,
        n.publisher_type,
        n.publisher_resource as publisher_url,
        nullif(regexp_extract(n.accrual_periodicity, '/frequency/([A-Z0-9_]+)$', 1), '') as periodicity_code,
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
),

versions as (
    select
        *,
        count(*) over (partition by dataset_number) as harvest_records,
        rank() over (
            partition by dataset_number order by modified_at desc nulls last, harvest_modified_at desc nulls last
        ) as version_rank,
        count(*) over (
            partition by dataset_number, modified_at, harvest_modified_at
        ) as same_version
    from records
)

select
    dataset_number,
    dataset_id,
    identifier_url,
    landing_page,
    title_pl,
    publisher_number,
    publisher_name,
    publisher_type,
    publisher_url,
    periodicity_code,
    category_count,
    is_high_value,
    issued_at,
    modified_at,
    cast(timezone('UTC', modified_at) as date) as modified_on,
    harvest_issued_at,
    harvest_modified_at,
    country_code,
    distribution_count,
    harvest_records,
    case
        when same_version > 1
            then error('dane_gov_pl_datasets: dataset ' || dataset_number || ' has two records tied as newest version')
        else source_row_number
    end as source_row_number,
    source_version,
    raw_sha256
from versions
where version_rank = 1
