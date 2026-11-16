{# Crossref counts of the works whose deposited author affiliation names Poland or Polska (query.affiliation, rows=0
   requests: no work records), newest Landing copy of each request (bronze.crossref_facet_counts). Grain: entity ×
   facet × facet_value. works_pl_by_year: '_total' (all years) and 'published' (one row per publication year, the
   year as facet_value); works_pl_<year>: '_total' and the facets type-name, publisher-name and funder-doi of that
   year (funder_id = the Funder Registry id from the DOI URL). Counts as published; a facet Crossref cut at its
   value limit has is_truncated (fewer values returned than value-count, or exactly 1,000). #}
with newest as (
    select *
    from {{ source('crossref', 'crossref_facet_counts') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

returned as (
    select
        *,
        count(*) over (partition by entity, facet) as facet_values_returned
    from newest
)

select
    entity,
    case
        when entity = 'works_pl_by_year' and facet = 'published' then try_cast(facet_value as integer)
        when entity <> 'works_pl_by_year' then try_cast(regexp_extract(entity, '^works_pl_([0-9]{4})$', 1) as integer)
    end as publication_year,
    facet,
    facet_value,
    case when facet = 'funder-doi' then regexp_extract(facet_value, '10\.13039/(.+)$', 1) end as funder_id,
    try_cast(works as bigint) as works,
    try_cast(total_results as bigint) as total_results,
    try_cast(facet_value_count as integer) as facet_value_count,
    case when facet <> '_total' then facet_values_returned end as facet_values_returned,
    coalesce(
        facet <> '_total'
        and (facet_values_returned < try_cast(facet_value_count as integer) or facet_values_returned >= 1000),
        false
    ) as is_truncated,
    raw_sha256,
    source_version,
    ingested_at
from returned
