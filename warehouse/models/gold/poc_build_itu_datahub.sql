{{ config(alias='build_itu_datahub', tags=['gold_block']) }}
{# Gold build stage: ITU DataHub ICT indicators (register GL-031) from silver.itu_datahub_observations (World Bank
   Data360 dataset ITU_DH), block 'itu_dh'. One measure per indicator and published unit, annual only:
   measure_key itu_datahub:itu_dh:<UNIT_MEASURE>:<INDICATOR>, e.g. itu_datahub:itu_dh:PT_POP:IT_NET_USER (individuals
   using the internet, % of population) and itu_datahub:itu_dh:SB_10P2_HB:IT_NET_BBND (fixed-broadband subscriptions
   per 100 inhabitants) beside itu_datahub:itu_dh:SB:IT_NET_BBND (the count): the unit is in the measure because ITU
   publishes one indicator in several units, never converted here (D33). A UNIT_MULT other than 0 would be kept in the
   unit code (<unit>_E<mult>) and stay unmapped, never applied. The indicator-specific breakdowns (sex, age,
   urbanisation, skill level) go to breakdown_json with the source codes, '_Z' (not applicable) left out; sex also to
   sex_code. is_total only where every breakdown is the source's total '_T' or not applicable '_Z': the ICT skills
   (BASIC, ABOVE_BASIC) have no published total and no total series. Text indicators (UNIT_MEASURE CAT, IT_COMP_*) and
   rows without a number are not loaded; a row the source flags as estimate (E) or forecast (F) is not an observation
   and is left out; OBS_STATUS A (normal value) or empty is 'ok', P 'provisional', any other flag 'flagged'; a year
   after the build year is left out. Economies through poc_map_geography (section itu_datahub, static ISO3); a code
   without a Gold country (historic ANT, YUG, territories such as ASC) maps to _X with its code kept and is counted by
   build_check. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'itu_datahub'
),

obs as (
    select
        *,
        unit_measure || case when unit_mult = 0 then '' else '_E' || cast(unit_mult as varchar) end as unit_code
    from {{ ref('itu_datahub_observations') }}
    where
        value is not null
        and unit_measure <> 'CAT'
        and coalesce(obs_status, 'A') not in ('E', 'F')
        and year <= year(current_date)
)

select
    'itu_datahub' as source_system,
    'itu_dh' as block_id,
    'ITU DataHub: ICT indicators by economy (World Bank Data360 ITU_DH)' as block_name,
    'itu_datahub:itu_dh:' || o.unit_code || ':' || o.indicator as measure_key,
    o.indicator_label || ', ' || o.unit_measure_label as measure_name,
    o.indicator as measure_codes,
    o.unit_code,
    o.unit_measure_label as unit_name,
    o.ref_area as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    o.time_period as period_label,
    '{' || concat_ws(
        ',',
        case when o.sex <> '_Z' then '"sex":' || to_json(o.sex) end,
        case when o.age <> '_Z' then '"age":' || to_json(o.age) end,
        case when o.urbanisation <> '_Z' then '"urbanisation":' || to_json(o.urbanisation) end,
        case when o.comp_breakdown_1 <> '_Z' then '"comp_breakdown_1":' || to_json(o.comp_breakdown_1) end,
        case when o.comp_breakdown_2 <> '_Z' then '"comp_breakdown_2":' || to_json(o.comp_breakdown_2) end,
        case when o.comp_breakdown_3 <> '_Z' then '"comp_breakdown_3":' || to_json(o.comp_breakdown_3) end
    ) || '}' as breakdown_json,
    '{' || concat_ws(
        ',',
        case when o.sex <> '_Z' then '"sex":' || to_json(o.sex_label) end,
        case when o.age <> '_Z' then '"age":' || to_json(o.age_label) end,
        case when o.urbanisation <> '_Z' then '"urbanisation":' || to_json(o.urbanisation_label) end,
        case when o.comp_breakdown_1 <> '_Z' then '"comp_breakdown_1":' || to_json(o.comp_breakdown_1_label) end,
        case when o.comp_breakdown_2 <> '_Z' then '"comp_breakdown_2":' || to_json(o.comp_breakdown_2) end,
        case when o.comp_breakdown_3 <> '_Z' then '"comp_breakdown_3":' || to_json(o.comp_breakdown_3) end
    ) || '}' as breakdown_label_json,
    case o.sex when '_T' then 'T' when 'M' then 'M' when 'F' then 'F' end as sex_code,
    (
        o.sex in ('_T', '_Z') and o.age in ('_T', '_Z') and o.urbanisation in ('_T', '_Z')
        and o.comp_breakdown_1 in ('_T', '_Z') and o.comp_breakdown_2 in ('_T', '_Z')
        and o.comp_breakdown_3 in ('_T', '_Z')
    ) as is_total,
    o.value,
    case
        when coalesce(o.obs_status, 'A') = 'A' then 'ok'
        when o.obs_status = 'P' then 'provisional'
        else 'flagged'
    end as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
left join geo as g on o.ref_area = g.source_code
