{# Crossref Funder Registry entries located in Poland (the /funders location filter), newest Landing copy
   (bronze.crossref_funders). Grain: funder_id (the registry id; funder_doi = 10.13039/<id>). Names and alternative
   names are organisations' names as the registry publishes them; replaces / replaced_by are registry ids. #}
with newest as (
    select *
    from {{ source('crossref', 'crossref_funders') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    funder_id,
    '10.13039/' || funder_id as funder_doi,
    name,
    location,
    uri,
    nullif(alt_names, '') as alt_names,
    nullif(replaces, '') as replaces,
    nullif(replaced_by, '') as replaced_by,
    try_cast(total_results as integer) as registry_total,
    raw_sha256,
    source_version,
    ingested_at
from newest
