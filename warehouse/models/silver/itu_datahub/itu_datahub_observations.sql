{# ITU DataHub ICT indicators (register GL-031) as the World Bank republishes them in Data360 dataset ITU_DH, from the
   newest copy of entity itu_dh (bronze.itu_datahub_observations, file ITU_DH.csv): one row per published observation,
   codes and labels as published. Annual. Indicator-specific breakdowns as published: SEX, AGE and URBANISATION for the
   internet-use indicators, COMP_BREAKDOWN_1 (BASIC, ABOVE_BASIC) for the ICT skills; '_T' is the source's total,
   '_Z' not applicable. UNIT_MEASURE is part of the grain: one indicator is published in several units (subscriptions
   SB and per 100 inhabitants SB_10P2_HB; prices in USD, PPP and % of GNI per person). value is the published number
   when OBS_VALUE is one; the IT_COMP_* indicators (UNIT_MEASURE CAT) publish text ('Monopoly', 'Full competition
   (year when competition was introduced)', 'N/A', …), kept in value_text with value null and never cast to 0.
   unit_mult is kept as published (0 in every row of the Dev trial); nothing is rescaled (D33).
   Grain: indicator x ref_area x sex x age x urbanisation x comp_breakdown_1 x comp_breakdown_2 x comp_breakdown_3 x
   unit_measure x time_period. #}
with records as (
    select *
    from {{ source('itu_datahub', 'itu_datahub_observations') }}
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
    sex_label,
    age,
    age_label,
    urbanisation,
    urbanisation_label,
    comp_breakdown_1,
    comp_breakdown_1_label,
    comp_breakdown_2,
    comp_breakdown_3,
    unit_measure,
    unit_measure_label,
    try_cast(unit_mult as integer) as unit_mult,
    freq,
    time_period,
    case when regexp_full_match(time_period, '(19|20)[0-9]{2}') then cast(time_period as integer) end as year,
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
