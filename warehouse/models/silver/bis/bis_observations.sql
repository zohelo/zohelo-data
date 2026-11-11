{# BIS statistics (SDMX dataflows of the BIS Data Portal, one entity per selection): one row per series
   and period of the newest Landing copy of each entity, typed. Dimensions differ by dataflow, so they are
   kept as published in dimensions_json (id → code, file order); the area is REF_AREA, or BORROWERS_CTY in
   the credit dataflows (WS_TC, WS_CREDIT_GAP, WS_DSR). Attributes are taken by name; a series attribute
   the BIS sends on a separate row without FREQ and period (WS_EER UNIT_MEASURE, WS_TC titles) fills the
   observations of that series (FREQ is the first dimension of every BIS dataflow). Values as published
   (UNIT_MEASURE codes: 368 per cent per year, 882 index 2020 = 100, 770 % of GDP, …); 'NaN' is no value. #}
with source_rows as (
    select *
    from {{ source('bis', 'bis_observations') }}
    qualify row_number() over (partition by raw_sha256, source_row_number order by ingested_at) = 1
),

newest as (
    select
        *,
        string_split(series_dimensions, ',') as dims,
        string_split(series_key, '.') as codes,
        string_split(attribute_names, ',') as attrs,
        string_split(attribute_values, '|') as attr_values,
        regexp_replace(series_key, '^[^.]*\.', '') as series_rest
    from source_rows
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

series_rows as (  -- series attributes on rows without FREQ and period
    select
        entity,
        series_rest,
        any_value(nullif(attr_values[list_position(attrs, 'UNIT_MEASURE')], '')) as unit_measure,
        any_value(nullif(attr_values[list_position(attrs, 'UNIT_MULT')], '')) as unit_mult,
        any_value(nullif(trim(coalesce(
            nullif(attr_values[list_position(attrs, 'TITLE')], ''),
            attr_values[list_position(attrs, 'TITLE_TS')]
        )), '')) as title
    from newest
    where nullif(time_period, '') is null
    group by 1, 2
)

select
    o.entity,
    o.dataflow,
    regexp_extract(o.dataflow, ':([A-Z0-9_]+)\(', 1) as dataflow_id,
    o.series_key,
    o.codes[list_position(o.dims, 'FREQ')] as freq,
    coalesce(o.codes[list_position(o.dims, 'REF_AREA')], o.codes[list_position(o.dims, 'BORROWERS_CTY')]) as ref_area,
    cast(to_json(map_from_entries(list_zip(o.dims, o.codes))) as varchar) as dimensions_json,
    o.time_period,
    try_cast(nullif(nullif(o.obs_value, ''), 'NaN') as double) as obs_value,
    coalesce(nullif(o.attr_values[list_position(o.attrs, 'UNIT_MEASURE')], ''), s.unit_measure) as unit_measure,
    coalesce(nullif(o.attr_values[list_position(o.attrs, 'UNIT_MULT')], ''), s.unit_mult) as unit_mult,
    nullif(o.attr_values[list_position(o.attrs, 'OBS_STATUS')], '') as obs_status,
    coalesce(nullif(trim(coalesce(
        nullif(o.attr_values[list_position(o.attrs, 'TITLE')], ''),
        o.attr_values[list_position(o.attrs, 'TITLE_TS')]
    )), ''), s.title) as title,
    o.raw_sha256,
    o.source_version,
    o.ingested_at
from newest as o
left join series_rows as s on o.entity = s.entity and o.series_rest = s.series_rest
where nullif(o.time_period, '') is not null
