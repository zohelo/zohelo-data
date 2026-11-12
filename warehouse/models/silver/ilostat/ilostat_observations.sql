{# ILOSTAT indicators (bulk indicator files, one entity per indicator and frequency): one row per series
   and period of the newest Landing copy of each entity, typed. Dimensions are taken by name from
   series_dimensions (ref_area ISO 3166-1 alpha-3 or an ILO aggregate X01…, source = survey type and id,
   indicator, sex SEX_T/M/F/O, classif1/classif2 e.g. AGE_YTHADULT_Y15-24); frequency from the entity suffix;
   time as published (2025, 2025Q2, 2025M07). Values in the indicator's unit (rates in per cent); obs_status
   and notes as published. National (reported) series only: ILO modelled estimates are other indicators. #}
with source_rows as (
    select *
    from {{ source('ilostat', 'ilostat_observations') }}
    qualify row_number() over (partition by raw_sha256, source_row_number order by ingested_at) = 1
),

newest as (
    select
        *,
        string_split(series_dimensions, ',') as dims,
        string_split(series_key, '.') as codes,
        string_split(attribute_names, ',') as attrs,
        string_split(attribute_values, '|') as attr_values
    from source_rows
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    dataflow as indicator,
    upper(right(entity, 1)) as freq,
    codes[list_position(dims, 'ref_area')] as ref_area,
    codes[list_position(dims, 'source')] as source_code,
    codes[list_position(dims, 'sex')] as sex,
    nullif(codes[list_position(dims, 'classif1')], '') as classif1,
    nullif(codes[list_position(dims, 'classif2')], '') as classif2,
    series_key,
    time_period,
    try_cast(nullif(obs_value, '') as double) as obs_value,
    nullif(attr_values[list_position(attrs, 'obs_status')], '') as obs_status,
    nullif(attr_values[list_position(attrs, 'note_classif')], '') as note_classif,
    nullif(attr_values[list_position(attrs, 'note_indicator')], '') as note_indicator,
    nullif(attr_values[list_position(attrs, 'note_source')], '') as note_source,
    raw_sha256,
    source_version,
    ingested_at
from newest
