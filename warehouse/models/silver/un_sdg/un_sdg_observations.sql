{# UN Global SDG Indicators Database (register GL-018): every SDG series record of Poland and the other 26 EU countries
   (UN M49 codes of poc_m49_eu27) in the newest release, from the newest copy of entity alldata_csv
   (bronze.un_sdg_observations, the release's bulk CSV). The CSV repeats a record once per indicator it is listed under
   (DC_ODA_BDVDL under 15.a.1 and 15.b.1): such rows share their ObservationID and differ only in goal, target and
   indicator (28,532 ObservationIDs on 2 or 3 rows in release 2026.Q3.G.01, Dev probe 45935478568), so they are one
   row here with the indicators listed; an ObservationID whose rows differ in anything else stays on several rows and
   fails the unique test. value is the published number; 'NaN' (no value: every row under nature N, observation status
   M, O or Q) and the censored values '<2.5', '<100', '<0.5', '<0.1' keep value null and say so in value_kind, never 0.
   Source codes as published: units, nature (C, CA, E, G, M, N, NA), observation status, the disaggregation dimensions
   as JSON (name -> code). Grain: series_code x geo_area_code x time_period x dimensions. #}
with records as (
    select *
    from {{ source('un_sdg', 'un_sdg_observations') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

areas as (
    select cast(cast(m49 as integer) as varchar) as geo_area_code
    from ({{ poc_m49_eu27() }})
),

records_per_observation as (
    select
        r.entity,
        r.release_name,
        r.series_id,
        r.series_code,
        r.series_description,
        r.geo_area_code,
        r.geo_area_name,
        r.time_period,
        r.value,
        r.value_type,
        r.time_detail,
        r.upper_bound,
        r.lower_bound,
        r.base_period,
        r.source,
        r.footnote,
        r.observation_id,
        r.nature,
        r.units,
        r.unit_multiplier,
        r.observation_status,
        r.dimensions,
        r.raw_sha256,
        r.source_version,
        string_agg(distinct r.goal, ';' order by r.goal) as goals,
        string_agg(distinct r.target, ';' order by r.target) as targets,
        string_agg(distinct r.indicator, ';' order by r.indicator) as indicators,
        count(*) as listed_rows
    from records as r
    inner join areas as a on r.geo_area_code = a.geo_area_code
    group by all
)

select
    entity,
    release_name,
    cast(left(release_name, 4) as integer) as release_year,
    series_id,
    series_code,
    series_description,
    goals,
    targets,
    indicators,
    listed_rows,
    geo_area_code,
    geo_area_name,
    case when regexp_full_match(time_period, '(19|20)[0-9]{2}') then cast(time_period as integer) end as time_period,
    nullif(time_detail, 'NA') as time_detail,
    case
        when regexp_full_match(value, '-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?') then cast(value as double)
    end as value,
    value as value_text,
    case
        when regexp_full_match(value, '-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?') then 'number'
        when value = 'NaN' then 'no_value'
        when regexp_full_match(value, '<[0-9]+(\.[0-9]+)?') then 'below'
    end as value_kind,
    value_type,
    case
        when regexp_full_match(upper_bound, '-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?') then cast(upper_bound as double)
    end as upper_bound,
    case
        when regexp_full_match(lower_bound, '-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?') then cast(lower_bound as double)
    end as lower_bound,
    nullif(base_period, 'NA') as base_period,
    units as unit_code,
    nullif(unit_multiplier, 'NA') as unit_multiplier,
    nature,
    nullif(observation_status, 'NA') as observation_status,
    json_extract_string(dimensions, '$."Reporting Type"') as reporting_type,
    dimensions,
    nullif(source, 'NA') as source,
    nullif(footnote, 'NA') as footnote,
    observation_id,
    raw_sha256,
    source_version
from records_per_observation
