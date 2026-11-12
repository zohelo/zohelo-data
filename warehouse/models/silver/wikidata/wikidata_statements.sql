{# Wikidata statements of the items that hold a TERC code (P1653), from the newest Landing copy of each SPARQL
   selection (bronze.wikidata_bindings). Grain: one row per statement (statement_id): property P1653 (TERC code),
   P1082 (population), P2046 (area), P625 (coordinates), P856 (official website), every rank; plus one row per item
   and label or Wikipedia article (property label_pl, label_en, wikipedia_pl, wikipedia_en; statement_id
   '<QID>$<property>'). Qualifiers that repeat inside one statement multiply the SPARQL solutions; they are folded
   by an explicit rule: point_in_time, end_time and dissolved = the latest value, language_qids = all values,
   solutions = how many were folded. Values as published (area in its unit_qid: Q712226 km², Q35852 hectare). #}
with newest as (
    select *
    from {{ source('wikidata', 'wikidata_bindings') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

statements as (
    select
        entity,
        case entity
            when 'terc_units' then 'P1653' when 'population' then 'P1082' when 'area' then 'P2046'
            when 'coordinates' then 'P625' when 'website' then 'P856'
        end as property,
        regexp_extract(item, '/entity/(Q[0-9]+)$', 1) as qid,
        regexp_extract(statement, '/statement/(.+)$', 1) as statement_id,
        lower(regexp_extract(rank, '#([A-Za-z]+)Rank$', 1)) as rank,
        value as value_text,
        try_cast(value as double) as value_number,
        nullif(regexp_extract(unit, '/entity/(Q[0-9]+)$', 1), '') as unit_qid,
        max(try_cast(left(time, 10) as date)) as point_in_time,
        max(try_cast(left(end_time, 10) as date)) as end_time,
        string_agg(distinct regexp_extract(lang, '/entity/(Q[0-9]+)$', 1), ',' order by regexp_extract(lang, '/entity/(Q[0-9]+)$', 1)) as language_qids,
        max(try_cast(left(dissolved, 10) as date)) as dissolved,
        count(*) as solutions,
        raw_sha256,
        source_version
    from newest
    where entity <> 'names'
    group by all
),

names as (
    select
        entity,
        u.property,
        regexp_extract(n.item, '/entity/(Q[0-9]+)$', 1) as qid,
        regexp_extract(n.item, '/entity/(Q[0-9]+)$', 1) || '$' || u.property as statement_id,
        'normal' as rank,
        u.value_text,
        cast(null as double) as value_number,
        cast(null as varchar) as unit_qid,
        cast(null as date) as point_in_time,
        cast(null as date) as end_time,
        cast(null as varchar) as language_qids,
        cast(null as date) as dissolved,
        1 as solutions,
        n.raw_sha256,
        n.source_version
    from newest as n,
        unnest([
            {'property': 'label_pl', 'value_text': n.label_pl}, {'property': 'label_en', 'value_text': n.label_en},
            {'property': 'wikipedia_pl', 'value_text': n.article_pl}, {'property': 'wikipedia_en', 'value_text': n.article_en}
        ]) as t(u)
    where n.entity = 'names' and u.value_text is not null
)

select
    *,
    case when property = 'P625' then try_cast(regexp_extract(value_text, '^Point\(([-0-9.eE]+) ', 1) as double) end as lon,
    case when property = 'P625' then try_cast(regexp_extract(value_text, ' ([-0-9.eE]+)\)$', 1) as double) end as lat
from statements
union all by name
select *, cast(null as double) as lon, cast(null as double) as lat from names
