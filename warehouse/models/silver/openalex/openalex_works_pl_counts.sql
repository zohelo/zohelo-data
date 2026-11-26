{# OpenAlex (register CD-028, #2996): works with at least one authorship affiliated with an institution in Poland
   (filter institutions.country_code:PL; a work is counted once however many Polish institutions it has), by
   publication year, from the newest copy of entity works_pl (bronze.openalex_works_counts). selection: 'all' (every
   such work), 'open_access' (open_access.is_oa:true) or 'field' (primary_topic.field.id = field_id, one of the 26
   OpenAlex fields; a work's primary topic gives one field, so fields do not overlap, and works without a primary
   topic are in no field). Field and domain names from the field list of the same copy. Every publication year as
   published (OpenAlex holds years before 1900 and a few after the read); the year of the copy is still filling. A
   year or count that is not a whole number fails the build (OpenAlex prints no markers in group_by answers).
   Grain: selection x field_id x publication_year (field_id '_T' for 'all' and 'open_access'). #}
with records as (
    select *
    from {{ source('openalex', 'openalex_works_counts') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

fields as (
    select
        field_id,
        field_name,
        domain_id,
        domain_name
    from records
    where record_kind = 'field'
)

select
    case g.request
        when 'works_by_year' then 'all'
        when 'works_oa_by_year' then 'open_access'
        else 'field'
    end as selection,
    coalesce(g.field_id, '_T') as field_id,
    f.field_name,
    f.domain_id,
    f.domain_name,
    case
        when regexp_full_match(g.group_key, '[0-9]{1,4}') then cast(g.group_key as integer)
        else error('openalex: publication year ' || coalesce(g.group_key, 'null') || ' in ' || g.member)
    end as publication_year,
    case
        when regexp_full_match(g.works, '[0-9]+') then cast(g.works as bigint)
        else error('openalex: count ' || coalesce(g.works, 'null') || ' in ' || g.member)
    end as works,
    g.raw_sha256,
    g.source_version
from records as g
left join fields as f on g.field_id = f.field_id
where g.record_kind = 'group'
