{{ config(materialized='incremental', incremental_strategy='append',
          pre_hook="{% if is_incremental() %}delete from {{ this }} where raw_sha256 not in (select raw_sha256 from {{ ref('eurostat_dataset_versions') }}){% endif %}") }}
{# Eurostat observations, newest version of each dataset, typed (design: project doc
   claude/eurostat-silver-design.md). Incremental per Landing file (raw_sha256): each build
   adds at most var('eurostat_datasets_per_build') datasets not yet in Silver, and the
   pre-hook removes rows of datasets that have a newer version, so Silver converges over
   successive builds without one oversized rebuild.
   time_period is normalised to SDMX (2023Q1 → 2023-Q1, 2023M01 → 2023-01, 2023S1 → 2023-S1,
   2023W01 → 2023-W01); value ':' or empty → null; the series dimensions other than freq,
   unit and geo stay as published in breakdown_dimensions / breakdown_codes. #}
with todo as (
    select raw_sha256 from {{ ref('eurostat_dataset_versions') }}
    {% if is_incremental() %}
    where raw_sha256 not in (select distinct raw_sha256 from {{ this }})
    {% endif %}
    order by dataset_id
    limit {{ var('eurostat_datasets_per_build', 400) }}
),
rows as (
    select b.*
    from {{ source('eurostat', 'eurostat_full_observations') }} b
    where b.raw_sha256 in (select raw_sha256 from todo)
)
select
    dataset_id,
    freq_code,
    unit_code,
    geo_code,
    case
        when regexp_matches(period_key, '^\d{4}-?Q[1-4]$') then regexp_replace(period_key, '^(\d{4})-?Q([1-4])$', '\1-Q\2')
        when regexp_matches(period_key, '^\d{4}-?S[12]$') then regexp_replace(period_key, '^(\d{4})-?S([12])$', '\1-S\2')
        when regexp_matches(period_key, '^\d{4}-?W\d{2}$') then regexp_replace(period_key, '^(\d{4})-?W(\d{2})$', '\1-W\2')
        when regexp_matches(period_key, '^\d{4}M\d{2}$') then regexp_replace(period_key, '^(\d{4})M(\d{2})$', '\1-\2')
        else period_key
    end as time_period,
    period_key as period_source,
    try_cast(nullif(nullif(value_text, ':'), '') as double) as value,
    value_text,
    status_code,
    array_to_string(list_filter(list_zip(string_split(series_dimensions, ','), string_split(series_key, ',')),
                                x -> x[1] not in ('freq', 'unit', 'geo')).apply(x -> x[1]), ',') as breakdown_dimensions,
    array_to_string(list_filter(list_zip(string_split(series_dimensions, ','), string_split(series_key, ',')),
                                x -> x[1] not in ('freq', 'unit', 'geo')).apply(x -> x[2]), ',') as breakdown_codes,
    raw_sha256,
    source_version
from rows
