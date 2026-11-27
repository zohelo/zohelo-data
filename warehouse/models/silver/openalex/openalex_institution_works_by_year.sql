{# OpenAlex (register CD-028, #2996): works per year of each institution OpenAlex places in Poland, from
   counts_by_year of the newest copy of entity institutions_pl (bronze.openalex_institutions). OpenAlex lists the
   recent years only (2010 onwards in Dev probe 45816100460); a year it does not list has no row (not 0). works_count
   = works with an authorship affiliated with the institution published in the year, oa_works_count = those that are
   open access; a listed year with 0 works is a published count. counts_by_year's cited_by_count is not kept: it is 0
   for 2010 and 2011 at almost every institution (196 and 197 of the 200 largest), a year OpenAlex does not cover
   rather than a count. The year of the copy is still filling. Not additive over institutions (a work can have
   several). Grain: institution_id x year. #}
{% set shape = '[{"year": "BIGINT", "works_count": "BIGINT", "oa_works_count": "BIGINT"}]' %}
with records as (
    select *
    from {{ source('openalex', 'openalex_institutions') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

years as (
    select
        replace(r.institution_id, 'https://openalex.org/', '') as institution_id,
        unnest(from_json(r.counts_by_year_json, '{{ shape }}')) as c,
        r.raw_sha256,
        r.source_version
    from records as r
)

select
    y.institution_id,
    cast(struct_extract(y.c, 'year') as integer) as year,
    struct_extract(y.c, 'works_count') as works_count,
    struct_extract(y.c, 'oa_works_count') as oa_works_count,
    y.raw_sha256,
    y.source_version
from years as y
