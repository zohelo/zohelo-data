{{ config(severity='warn') }}
{# Wikidata TERC codes (P1653, current statements: not deprecated, no end time, item not dissolved) held by more
   than one item. They are different entities: silver.wikidata_terc_attributes keeps such a code only when exactly
   one item has a preferred-rank statement, otherwise leaves it out of Silver and Gold. One row per code (warn). #}
select
    value_text as terc_code,
    count(distinct qid) as items,
    count(distinct qid) filter (where rank = 'preferred') as preferred_items,
    string_agg(distinct qid, ',') as qids
from {{ ref('wikidata_statements') }}
where property = 'P1653' and rank <> 'deprecated' and end_time is null and dissolved is null
group by value_text
having count(distinct qid) > 1
