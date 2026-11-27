{# World Bank Enterprise Surveys indicators (register GL-010) as published in World Bank Data360 dataset WB_ES, from the
   newest copy of entity wb_es (bronze.world_bank_es_observations, file WB_ES.csv): one row per published
   observation, codes and labels as published. Survey estimates by economy (ISO3) and survey year (irregular years,
   TIME_PERIOD published as '2023.0'; year is its integer). Indicator codes WB_ES_T_* cover all firms, WB_ES_M_* and
   WB_ES_S_* only manufacturing or services firms. Breakdowns as published: COMP_BREAKDOWN_1 sector, COMP_BREAKDOWN_2
   firm size, COMP_BREAKDOWN_3 other firm characteristics (exporter, ownership, …); '_T' is the source's total, '_Z'
   not applicable; SEX, AGE and URBANISATION are '_Z' in every row of the Dev trial and kept for the shared layout.
   value is the published number when OBS_VALUE is one, else null (never 0; the trial found no other value).
   unit_mult is kept as published (0 in every row); nothing is rescaled (D33).
   Grain: indicator x ref_area x sex x age x urbanisation x comp_breakdown_1 x comp_breakdown_2 x comp_breakdown_3 x
   unit_measure x time_period. #}
with records as (
    select *
    from {{ source('world_bank_es', 'world_bank_es_observations') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    database_id,
    indicator,
    indicator_label,
    ref_area,
    ref_area_label,
    sex,
    age,
    urbanisation,
    comp_breakdown_1,
    comp_breakdown_1_label,
    comp_breakdown_2,
    comp_breakdown_2_label,
    comp_breakdown_3,
    comp_breakdown_3_label,
    unit_measure,
    unit_measure_label,
    try_cast(unit_mult as integer) as unit_mult,
    freq,
    time_period,
    case
        when regexp_full_match(time_period, '(19|20)[0-9]{2}(\.0)?') then cast(left(time_period, 4) as integer)
    end as year,
    case
        when regexp_full_match(obs_value, '-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?') then cast(obs_value as double)
    end as value,
    obs_value as value_text,
    try_cast(decimals as integer) as decimals,
    obs_status,
    obs_status_label,
    obs_conf,
    agg_method,
    unit_type,
    raw_sha256,
    source_version
from records
