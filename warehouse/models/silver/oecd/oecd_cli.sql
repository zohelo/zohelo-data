{# OECD composite leading indicators (dataflow DSD_STES@DF_CLI): one row per series and period of the
   newest Landing copy of entity cli, typed. Dimension codes are taken by name from series_dimensions
   (the column order of the file), so a reordered dataflow version still maps; attributes likewise.
   measure: LI (composite leading indicator), BCICP / CCICP (business / consumer confidence),
   RS (reference series) and LOCO* (component series); adjustment AA = amplitude adjusted;
   methodology H = harmonised. Values are index points as published. #}
with source_rows as (
    select *
    from {{ source('oecd', 'oecd_observations') }}
    where entity = 'cli'
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
    dataflow,
    codes[list_position(dims, 'REF_AREA')] as ref_area,
    codes[list_position(dims, 'FREQ')] as freq,
    codes[list_position(dims, 'MEASURE')] as measure,
    codes[list_position(dims, 'UNIT_MEASURE')] as unit_measure,
    codes[list_position(dims, 'ADJUSTMENT')] as adjustment,
    codes[list_position(dims, 'TRANSFORMATION')] as transformation,
    codes[list_position(dims, 'METHODOLOGY')] as methodology,
    time_period,
    try_cast(nullif(obs_value, '') as double) as obs_value,
    nullif(attr_values[list_position(attrs, 'OBS_STATUS')], '') as obs_status,
    series_key,
    raw_sha256,
    source_version,
    ingested_at
from newest
