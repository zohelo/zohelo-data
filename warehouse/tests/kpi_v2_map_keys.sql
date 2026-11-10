-- kpi_v2_map (O31): one row per v2 key and source; a v2 key is fed by one concept and a concept feeds one
-- v2 key (published contract v2). Returns the offending keys.
with m as (select * from {{ ref('kpi_v2_map') }}),

duplicate_rows as (
    select
        v2_key,
        'source ' || source_key || ' listed twice' as problem
    from m
    group by v2_key, source_key
    having count(*) > 1
),

concepts_per_key as (
    select
        v2_key,
        'fed by ' || count(distinct concept_key) || ' concepts' as problem
    from m
    where coalesce(concept_key, '') <> ''
    group by v2_key
    having count(distinct concept_key) > 1
),

keys_per_concept as (
    select
        concept_key as v2_key,
        'concept feeds ' || count(distinct v2_key) || ' v2 keys' as problem
    from m
    where coalesce(concept_key, '') <> ''
    group by concept_key
    having count(distinct v2_key) > 1
)

select * from duplicate_rows
union all
select * from concepts_per_key
union all
select * from keys_per_concept
