{# Wikidata attributes of Polish territorial units: one row per TERC code of the current TERYT catalogue
   (silver.teryt_terc) × property, for the codes Wikidata holds. Codes Wikidata has but TERYT does not (abolished
   units, other code shapes) are left out; the Dev trial counts them.
   Item rule: among the items whose P1653 statement carries the code (not deprecated, no end time P582, item not
   dissolved P576), the preferred-rank statement first, then the lowest numeric QID; item_candidates counts them.
   Value rule, per property of that item: deprecated statements and websites with an end time are left out; then
   preferred rank first, the latest point in time (P585; none last), for websites one in Polish (Q809) or without
   a language first, and the statement id as the last tie-break; candidates counts the statements considered,
   tied those with the chosen one's rank and point in time (tied > 1: the statement id decided). Properties: qid,
   label_pl, label_en, wikipedia_pl, wikipedia_en, population, area (value in unit_qid), coordinates (lon, lat), website. #}
with statements as (select * from {{ ref('wikidata_statements') }}),

codes as (
    select
        s.value_text as terc_code,
        t.unit_level,
        s.qid,
        s.rank,
        s.raw_sha256,
        s.source_version,
        count(*) over (partition by s.value_text) as item_candidates,
        row_number() over (
            partition by s.value_text order by s.rank = 'preferred' desc, cast(substr(s.qid, 2) as bigint)
        ) as pick
    from statements as s
    inner join {{ ref('teryt_terc') }} as t on s.value_text = t.terc_code
    where s.property = 'P1653' and s.rank <> 'deprecated' and s.end_time is null and s.dissolved is null
),

units as (
    select * from codes
    where pick = 1
),

candidates as (
    select
        u.terc_code,
        u.unit_level,
        u.qid,
        u.item_candidates,
        case s.property
            when 'P1082' then 'population' when 'P2046' then 'area' when 'P625' then 'coordinates'
            when 'P856' then 'website' else s.property
        end as property,
        s.statement_id,
        s.rank,
        s.value_text,
        s.value_number,
        s.unit_qid,
        s.point_in_time,
        s.lon,
        s.lat,
        s.raw_sha256,
        s.source_version,
        count(*) over (partition by u.terc_code, s.property) as candidates,
        count(*) over (partition by u.terc_code, s.property, s.rank, s.point_in_time) as tied,
        row_number() over (
            partition by u.terc_code, s.property
            order by
                s.rank = 'preferred' desc,
                s.point_in_time desc nulls last,
                coalesce(s.language_qids, 'Q809') like '%Q809%' desc,
                s.statement_id
        ) as pick
    from units as u
    inner join statements as s on u.qid = s.qid
    where
        s.property <> 'P1653'
        and s.rank <> 'deprecated'
        and not (s.property = 'P856' and s.end_time is not null)
)

select
    terc_code,
    unit_level,
    property,
    qid,
    statement_id,
    rank,
    value_text,
    value_number,
    unit_qid,
    point_in_time,
    lon,
    lat,
    candidates,
    tied,
    raw_sha256,
    source_version
from candidates
where pick = 1
union all
select
    terc_code,
    unit_level,
    'qid' as property,
    qid,
    null as statement_id,
    rank,
    qid as value_text,
    null as value_number,
    null as unit_qid,
    null as point_in_time,
    null as lon,
    null as lat,
    item_candidates as candidates,
    1 as tied,
    raw_sha256,
    source_version
from units
