{{ config(alias='build_gpw_ebi', tags=['gold_block']) }}
{# Gold build stage: EBI issuer reports published by GPW (silver.gpw_ebi_reports) counted per month of
   publication (Warsaw time as listed), Poland. Fact kind: event counts. measure_key gpw_ebi:reports:reports,
   monthly only; breakdown category (as listed: Current, Quarterly, …) and the total (category _T, is_total). Only
   complete months: after the month of the oldest report held (the first run reads back to a floor, so that month
   can be partial) and before the month of the newest report held (and the current month): Gold builds weekly and
   the list is read on Fridays, so the calendar alone would count a month whose last days are unread; a report
   published after the month proves a read after it ended, and each read takes everything from the newest held day
   down, so the month was read to its end. Few EBI reports a month, so a month can wait for the next report. A month without a report held has no row, never a 0:
   an empty answer of the list cannot be told from a month without reports. A report is counted once, on its day of
   publication; a correction is its own report. Provenance: the newest version and the newest day document of the
   month. #}
with reports as (
    select
        published_date,
        category,
        source_version,
        raw_sha256,
        ingested_at
    from {{ ref('gpw_ebi_reports') }}
),

window_months as (
    select
        cast(date_trunc('month', min(published_date)) + interval 1 month as date) as first_month,
        cast(least(date_trunc('month', current_date), date_trunc('month', max(published_date))) as date) as open_month
    from reports
),

by_category as (
    select
        cast(date_trunc('month', r.published_date) as date) as date_key,
        r.category,
        count(*) as reports,
        max(r.source_version) as source_version,
        max(r.ingested_at) as ingested_at,
        arg_max(r.raw_sha256, r.ingested_at) as raw_sha256
    from reports as r
    cross join window_months as w
    where r.published_date >= w.first_month and r.published_date < w.open_month
    group by date_trunc('month', r.published_date), r.category
),

counted as (
    select
        date_key,
        category,
        reports,
        source_version,
        raw_sha256
    from by_category
    union all
    select
        date_key,
        '_T' as category,
        sum(reports) as reports,
        max(source_version) as source_version,
        arg_max(raw_sha256, ingested_at) as raw_sha256
    from by_category
    group by date_key
)

select
    'gpw_ebi' as source_system,
    'reports' as block_id,
    'EBI issuer reports published by GPW' as block_name,
    'gpw_ebi:reports:reports' as measure_key,
    'EBI issuer reports published by GPW per month' as measure_name,
    'reports' as measure_codes,
    'reports' as unit_code,
    'reports' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'M' as frequency,
    date_key,
    strftime(date_key, '%Y-%m') as period_label,
    cast(json_object('category', category) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    category = '_T' as is_total,
    cast(reports as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from counted
