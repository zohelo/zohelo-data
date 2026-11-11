{# UNECE Statistical Database, economic statistics (PXWeb tables as JSON-stat): one row per table, series and
   period of the newest Landing copy of each table (or table part <table>_p<n>), typed. Variable value codes
   are kept as published in dimensions_json (variable → code) and resolved to their labels from
   bronze.unece_codes in labels_json; the area is the Country or Country/Region variable (UN M49 numeric
   codes, e.g. 616 Poland, and UNECE aggregates such as E27 = European Union-27). time_period is the
   published period label (1990, 2025Q2, 2025M07). Values as published in the table's measurement unit. #}
with source_rows as (
    select *
    from {{ source('unece', 'unece_observations') }}
    qualify row_number() over (partition by raw_sha256, source_row_number order by ingested_at) = 1
),

newest as (
    select
        *,
        string_split(series_dimensions, ',') as dims,
        string_split(series_key, '.') as codes
    from source_rows
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

codes as (
    select
        raw_sha256,
        dimension,
        code,
        any_value(label) as label
    from {{ source('unece', 'unece_codes') }}
    group by all
),

exploded as (  -- one row per cell and variable
    select
        n.*,
        d.i
    from newest as n, unnest(range(1, len(n.dims) + 1)) as d (i)
),

cells as (
    select
        e.entity,
        e.dataflow,
        e.series_key,
        e.time_period,
        e.obs_value,
        e.attribute_values,
        e.raw_sha256,
        e.source_version,
        e.ingested_at,
        e.dims,
        e.codes,
        list(c.label order by e.i) as labels
    from exploded as e
    left join codes as c on e.raw_sha256 = c.raw_sha256 and c.dimension = e.dims[e.i] and c.code = e.codes[e.i]
    group by all
)

select
    entity,
    regexp_replace(entity, '_p[0-9]+$', '') as table_id,
    dataflow as table_title,
    series_key,
    coalesce(codes[list_position(dims, 'Country')], codes[list_position(dims, 'Country/Region')]) as area_code,
    coalesce(labels[list_position(dims, 'Country')], labels[list_position(dims, 'Country/Region')]) as area_name,
    cast(to_json(map_from_entries(list_zip(dims, codes))) as varchar) as dimensions_json,
    cast(to_json(map_from_entries(list_zip(dims, labels))) as varchar) as labels_json,
    time_period,
    try_cast(obs_value as double) as obs_value,
    nullif(attribute_values, '') as obs_status,
    raw_sha256,
    source_version,
    ingested_at
from cells
