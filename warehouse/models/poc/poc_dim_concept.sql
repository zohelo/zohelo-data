{{ config(alias='dim_concept') }}
{# Gold PoC: cross-source concepts (warehouse/macros/poc_blocks.sql poc_concepts). #}
select
    concept_key,
    any_value(name_en) as name_en,
    any_value(name_pl) as name_pl,
    any_value(unit) as unit,
    count(*) as measures,
    list(distinct source_system) as sources
from ({{ poc_concepts() }})
group by concept_key
