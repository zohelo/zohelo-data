{{ config(alias='concept_measures') }}
{# Gold PoC: which measures belong to which concept, with the source's definition of it. #}
with concepts as ({{ poc_concepts() }}),

measures as (
    select
        measure_key,
        source_system
    from {{ ref('poc_dim_measure') }}
    where activated
)

select
    c.concept_key,
    m.measure_key,
    c.source_system,
    c.definition,
    c.measure_key_pattern
from concepts as c
inner join measures as m on c.source_system = m.source_system and m.measure_key like c.measure_key_pattern
