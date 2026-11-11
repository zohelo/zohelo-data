{# UN Statistical Yearbook tables (UNdata, newest edition of each table): one row per table, area, year and
   series, typed. area_code is the UN M49 code (1 = world, 616 = Poland; regions are M49 aggregates); the value
   is published with thousands separators ("7,021.73"), removed before the cast; series carry their unit in
   the name (e.g. "(millions)", "(percentage)"). Footnotes and the source agency text as published. #}
with source_rows as (
    select *
    from {{ source('undata', 'undata_syb_values') }}
    qualify row_number() over (partition by raw_sha256, source_row_number order by ingested_at) = 1
),

newest as (
    select *
    from source_rows
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    table_code,
    table_title,
    area_code,
    area_name,
    try_cast(year as integer) as year,
    series,
    try_cast(replace(nullif(trim(value), ''), ',', '') as double) as value,
    nullif(value, '') as value_text,
    nullif(footnotes, '') as footnotes,
    nullif(source_text, '') as source_text,
    raw_sha256,
    source_version,
    ingested_at
from newest
