{# UNDP Human Development Report composite indices (Human Development Data Center, complete time series CSV of
   the newest report): one row per country or aggregate, indicator and year, typed. The source column name
   is split into the indicator code and the year (hdi_2023 → hdi, 2023; le_f_2023 → le_f; gii_rank_2023 →
   gii_rank). Indicators: hdi, le (life expectancy at birth), eys, mys (expected / mean years of schooling),
   gnipc (GNI per capita, 2021 PPP $), gdi, gii, ihdi, phdi, coef_ineq, … and their sex (_f, _m) variants;
   ranks as published. The whole series is recalculated with each report, so compare within one release. #}
with source_rows as (
    select *
    from {{ source('undp_hdr', 'undp_hdr_values') }}
    qualify row_number() over (partition by raw_sha256, source_row_number, column_name order by ingested_at) = 1
),

newest as (
    select *
    from source_rows
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    iso3,
    country,
    nullif(hdicode, '') as hdicode,
    nullif(region, '') as region,
    regexp_extract(column_name, '^(.*)_([0-9]{4})$', 1) as indicator,
    cast(regexp_extract(column_name, '_([0-9]{4})$', 1) as integer) as year,
    column_name,
    try_cast(value as double) as value,
    raw_sha256,
    source_version,
    ingested_at
from newest
where regexp_matches(column_name, '_[0-9]{4}$')
