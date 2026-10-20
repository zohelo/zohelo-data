-- Silver: Eurostat observations, typed. Applied by the loader to ONE Bronze file (one dataset
-- version) at a time; `bronze` is that file. The result replaces the dataset's partition in
-- silver.eurostat_observations, so Silver always holds exactly the newest version of every
-- dataset (design #875). Columns kept as published stay in Bronze (value_text, period_key).
--   time_period: SDMX format (2023Q1 -> 2023-Q1, 2023M01 -> 2023-01, 2023S1 -> 2023-S1, 2023W01 -> 2023-W01)
--   value: null for ':' or empty; status_code: Eurostat flags as published
--   breakdown_dimensions / breakdown_codes: the series dimensions other than freq, unit and geo
select
    dataset_id,
    freq_code,
    unit_code,
    geo_code,
    case
        when regexp_matches(period_key, '^\d{4}-?Q[1-4]$')
            then regexp_replace(period_key, '^(\d{4})-?Q([1-4])$', '\1-Q\2')
        when regexp_matches(period_key, '^\d{4}-?S[12]$')
            then regexp_replace(period_key, '^(\d{4})-?S([12])$', '\1-S\2')
        when regexp_matches(period_key, '^\d{4}-?W\d{2}$')
            then regexp_replace(period_key, '^(\d{4})-?W(\d{2})$', '\1-W\2')
        when regexp_matches(period_key, '^\d{4}M\d{2}$')
            then regexp_replace(period_key, '^(\d{4})M(\d{2})$', '\1-\2')
        else period_key
    end as time_period,
    try_cast(nullif(nullif(value_text, ':'), '') as double) as value,
    status_code,
    array_to_string(list_filter(
        list_zip(string_split(series_dimensions, ','), string_split(series_key, ',')),
        x -> x[1] not in ('freq', 'unit', 'geo')
    ).apply(x -> x[1]), ',') as breakdown_dimensions,
    array_to_string(list_filter(
        list_zip(string_split(series_dimensions, ','), string_split(series_key, ',')),
        x -> x[1] not in ('freq', 'unit', 'geo')
    ).apply(x -> x[2]), ',') as breakdown_codes,
    raw_sha256,
    source_version
from bronze
