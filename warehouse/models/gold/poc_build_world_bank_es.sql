{{ config(alias='build_world_bank_es', tags=['gold_block']) }}
{# Gold build stage: World Bank Enterprise Surveys (register GL-010) from silver.world_bank_es_observations (World Bank
   Data360 dataset WB_ES), block 'wb_es'. The smallest valuable selection: the all-firms indicators (WB_ES_T_*) of
   every economy and survey year, totals only (sector, firm size and the other firm characteristics all '_T').
   Not loaded here (they stay in Silver): the breakdowns of WB_ES_T_* by major sector, size, exporter and ownership
   type, and the WB_ES_M_* / WB_ES_S_* indicators asked of manufacturing or services firms only (they have no
   all-firms total). One measure per indicator, measure_key world_bank_es:wb_es:<UNIT_MEASURE>:<INDICATOR>, e.g.
   world_bank_es:wb_es:PT_F:WB_ES_T_GRAFT3 (bribery incidence, % of firms); each indicator has one unit, values and
   units as published (D33; a UNIT_MULT other than 0 would stay in the unit code, <unit>_E<mult>, unmapped). Annual,
   date_key = 1 January of the survey year (survey years are irregular per economy). The survey estimates are
   published values; a published 0 is kept, a row without a number is not loaded (none in the trial); a row the
   source flags as estimate (E) or forecast (F) is left out, OBS_STATUS A or empty is 'ok', P 'provisional', any
   other flag 'flagged'; a year after the build year is left out. Economies through poc_map_geography (section
   world_bank_es, static ISO3); a code without a Gold country maps to _X with its code kept, counted by build_check. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'world_bank_es'
),

obs as (
    select
        *,
        unit_measure || case when unit_mult = 0 then '' else '_E' || cast(unit_mult as varchar) end as unit_code
    from {{ ref('world_bank_es_observations') }}
    where
        indicator like 'WB\_ES\_T\_%' escape '\'
        and comp_breakdown_1 = '_T'
        and comp_breakdown_2 = '_T'
        and comp_breakdown_3 = '_T'
        and value is not null
        and coalesce(obs_status, 'A') not in ('E', 'F')
        and year <= year(current_date)
)

select
    'world_bank_es' as source_system,
    'wb_es' as block_id,
    'World Bank Enterprise Surveys: all-firms indicators by economy (Data360 WB_ES)' as block_name,
    'world_bank_es:wb_es:' || o.unit_code || ':' || o.indicator as measure_key,
    o.indicator_label || ', ' || o.unit_measure_label as measure_name,
    o.indicator as measure_codes,
    o.unit_code,
    o.unit_measure_label as unit_name,
    o.ref_area as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    '{"comp_breakdown_1":"_T","comp_breakdown_2":"_T","comp_breakdown_3":"_T"}' as breakdown_json,
    '{"comp_breakdown_1":' || to_json(o.comp_breakdown_1_label) || ',"comp_breakdown_2":'
    || to_json(o.comp_breakdown_2_label) || ',"comp_breakdown_3":' || to_json(o.comp_breakdown_3_label)
    || '}' as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
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
