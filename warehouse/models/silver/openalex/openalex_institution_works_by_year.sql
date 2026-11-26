{# OpenAlex (register CD-028, #2996): works and citations per year of each institution OpenAlex places in Poland,
   from counts_by_year of the newest copy of entity institutions_pl (bronze.openalex_institutions). OpenAlex gives
   these for the most recent years only (about ten); a year it does not list has no row (not 0). works_count = works
   with an authorship affiliated with the institution published in the year; cited_by_count = citations the
   institution's works received in the year (a year can list citations and 0 works; both are published counts, not
   markers). The year of the copy is still filling. Counts are not additive over institutions (a work can have
   several). Grain: institution_id x year. #}
with records as (
    select *
    from {{ source('openalex', 'openalex_institutions') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

years as (
    select
        replace(institution_id, 'https://openalex.org/', '') as institution_id,
        unnest(from_json(counts_by_year_json, '[{"year": "BIGINT", "works_count": "BIGINT", "cited_by_count": "BIGINT"}]'))
            as c,
        raw_sha256,
        source_version
    from records
)

select
    institution_id,
    cast(c.year as integer) as year,
    c.works_count,
    c.cited_by_count,
    raw_sha256,
    source_version
from years
