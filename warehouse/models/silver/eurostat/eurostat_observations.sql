{{ config(materialized='incremental', incremental_strategy='append',
          pre_hook="{% if is_incremental() %}{{ eurostat_clear() }}{% endif %}") }}
{# Eurostat observations, newest version of each dataset, typed (design: warehouse/README.md,
   "Eurostat Silver"). Incremental per Landing file (raw_sha256): the batch
   (vars eurostat_batch_*) is planned by warehouse/eurostat_batch.py from Iceberg metadata,
   up to a row budget per backfill build (#822); the pre-hook removes superseded versions. Silver converges
   over successive builds without one oversized rebuild. The literal dataset_id filter
   lets the scan prune Bronze partitions.
   time_period is normalised to SDMX (2023Q1 → 2023-Q1, 2023M01 → 2023-01, 2023S1 → 2023-S1,
   2023W01 → 2023-W01); value ':' or empty → null; the series dimensions other than freq,
   unit and geo stay as published in breakdown_dimensions / breakdown_codes. #}
with rows as (
    select *
    from {{ source('eurostat', 'eurostat_full_observations') }}
    where {{ eurostat_batch_filter() }}
)

select
    dataset_id,
    freq_code,
    unit_code,
    geo_code,
    case
        when
            regexp_matches(period_key, '^\d{4}-?Q[1-4]$')
            then regexp_replace(period_key, '^(\d{4})-?Q([1-4])$', '\1-Q\2')
        when
            regexp_matches(period_key, '^\d{4}-?S[12]$')
            then regexp_replace(period_key, '^(\d{4})-?S([12])$', '\1-S\2')
        when
            regexp_matches(period_key, '^\d{4}-?W\d{2}$')
            then regexp_replace(period_key, '^(\d{4})-?W(\d{2})$', '\1-W\2')
        when regexp_matches(period_key, '^\d{4}M\d{2}$') then regexp_replace(period_key, '^(\d{4})M(\d{2})$', '\1-\2')
        else period_key
    end as time_period,
    period_key as period_source,
    try_cast(nullif(nullif(value_text, ':'), '') as double) as value,
    value_text,
    status_code,
    array_to_string(list_filter(
        list_zip(string_split(series_dimensions, ','), string_split(series_key, ',')),
        x -> x[1] not in ('freq', 'unit', 'geo')
    ).apply (x -> x[1]
    ), ',') as breakdown_dimensions,
    array_to_string(list_filter(
        list_zip(string_split(series_dimensions, ','), string_split(series_key, ',')),
        x -> x[1] not in ('freq', 'unit', 'geo')
    ).apply (x -> x[2]
    ), ',') as breakdown_codes,
    raw_sha256,
    source_version
from rows
