{# ECB Data Portal observations (SDMX csvdata) from bronze.ecb_observations: one row per series and period of the
   newest Landing copy of every entity, typed. Dimension codes are taken by name from series_dimensions (the
   column order of the file); the dimensions JSON keeps every code by name, so a flow's own dimensions (EXR
   CURRENCY, ICP/HICP item, BSI BS_ITEM…) stay available. period_start: first day of the period
   (D and B: the day; M: the month; Q: the quarter; A: the year). Values as published; unit_mult is the power of
   ten of the unit (BSI stocks: 6 = millions). #}
with source_rows as (
    select *
    from {{ source('ecb', 'ecb_observations') }}
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
),

typed as (
    select
        entity,
        dataflow,
        series_key,
        codes[list_position(dims, 'FREQ')] as freq,
        codes[list_position(dims, 'REF_AREA')] as ref_area,
        codes[list_position(dims, 'CURRENCY')] as currency,
        cast(to_json(map(dims, codes)) as varchar) as dimensions,
        time_period,
        try_cast(nullif(obs_value, '') as double) as obs_value,
        nullif(attr_values[list_position(attrs, 'OBS_STATUS')], '') as obs_status,
        coalesce(
            nullif(attr_values[list_position(attrs, 'UNIT')], ''),
            nullif(attr_values[list_position(attrs, 'UNIT_MEASURE')], '')
        ) as unit,  -- EST names it UNIT_MEASURE
        try_cast(nullif(attr_values[list_position(attrs, 'UNIT_MULT')], '') as integer) as unit_mult,
        try_cast(nullif(attr_values[list_position(attrs, 'DECIMALS')], '') as integer) as decimals,
        nullif(attr_values[list_position(attrs, 'TITLE')], '') as title,
        nullif(attr_values[list_position(attrs, 'TITLE_COMPL')], '') as title_compl,
        raw_sha256,
        source_version,
        ingested_at
    from newest
)

select
    *,
    case
        when length(time_period) = 10 then try_cast(time_period as date)
        when regexp_matches(time_period, '^[0-9]{4}-[0-9]{2}$') then try_cast(time_period || '-01' as date)
        when regexp_matches(time_period, '^[0-9]{4}-Q[1-4]$')
            then make_date(cast(left(time_period, 4) as integer), cast(right(time_period, 1) as integer) * 3 - 2, 1)
        when regexp_matches(time_period, '^[0-9]{4}$') then make_date(cast(time_period as integer), 1, 1)
    end as period_start
from typed
