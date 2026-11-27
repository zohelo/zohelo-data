{{ config(alias='build_uokik_decisions', tags=['gold_block']) }}
{# Gold build stage: decisions of the President of UOKiK in the decisions database (silver.uokik_decisions, register
   PLG-020 and PLG-021) counted per year of the decision date (A), Poland. Fact kind: event counts; frequency A only.
   Blocks and measures (one measure_key per breakdown, unit decisions):
   - decisions: uokik_decisions:decisions:decisions:total:annual (all decisions, is_total) and
     uokik_decisions:decisions:decisions:category:annual ({"category": <subject area as listed>});
   - concentration: uokik_decisions:concentration:decisions:total:annual, the decisions whose number carries the
     signature DKK (Departament Kontroli Koncentracji).
   A decision is counted once per year (distinct @unid), in its category. Years without a decision have no row
   (never a 0); a decision without a year is not counted. The year of the newest download is provisional (decisions
   are added during the year). raw_sha256 = the newest category document counted. #}
with decisions as (
    select
        unid,
        decision_year,
        category,
        is_concentration,
        source_version,
        raw_sha256,
        ingested_at
    from {{ ref('uokik_decisions') }}
    where decision_year is not null
),

snapshot as (
    select year(cast(strptime(max(source_version), '%Y%m%dT%H%M%SZ') as date)) as snapshot_year
    from decisions
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'uokik_decisions'
),

counted as (
    select
        'decisions' as block_id,
        'total' as breakdown_code,
        decision_year,
        '{}' as breakdown_json,
        count(distinct unid) as decisions,
        max(source_version) as source_version,
        arg_max(raw_sha256, ingested_at) as raw_sha256
    from decisions
    group by decision_year
    union all
    select
        'decisions' as block_id,
        'category' as breakdown_code,
        decision_year,
        cast(json_object('category', category) as varchar) as breakdown_json,
        count(distinct unid) as decisions,
        max(source_version) as source_version,
        arg_max(raw_sha256, ingested_at) as raw_sha256
    from decisions
    group by decision_year, category
    union all
    select
        'concentration' as block_id,
        'total' as breakdown_code,
        decision_year,
        '{}' as breakdown_json,
        count(distinct unid) as decisions,
        max(source_version) as source_version,
        arg_max(raw_sha256, ingested_at) as raw_sha256
    from decisions
    where is_concentration
    group by decision_year
)

select
    'uokik_decisions' as source_system,
    c.block_id,
    case c.block_id
        when 'decisions' then 'Decisions of the President of UOKiK (decisions database)'
        else 'Concentration decisions of the President of UOKiK (DKK)'
    end as block_name,
    'uokik_decisions:' || c.block_id || ':decisions:' || c.breakdown_code || ':annual' as measure_key,
    case
        when c.block_id = 'concentration' then 'Concentration decisions of the President of UOKiK per year'
        when c.breakdown_code = 'category' then 'Decisions of the President of UOKiK per year, by subject area'
        else 'Decisions of the President of UOKiK per year'
    end as measure_name,
    c.breakdown_code || ':annual' as measure_codes,
    'decisions' as unit_code,
    'decisions' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(c.decision_year, 1, 1) as date_key,
    cast(c.decision_year as varchar) as period_label,
    c.breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.breakdown_code = 'total' as is_total,
    cast(c.decisions as double) as value,
    case when c.decision_year >= s.snapshot_year then 'provisional' else 'ok' end as observation_status_key,
    c.source_version,
    c.raw_sha256
from counted as c
cross join snapshot as s
left join geo_map as g on g.source_code = 'PL'
