{{ config(alias='build_gpw_ebi', tags=['gold_block']) }}
{# Gold build stage: EBI issuer reports published by GPW (silver.gpw_ebi_reports) counted per month of
   publication (Warsaw time as listed), Poland. Fact kind: event counts. measure_key gpw_ebi:reports:reports,
   monthly only; breakdown category (as listed: Current, Quarterly, …) and the total (category _T, is_total). Only
   complete months: after the month of the oldest report held (the first run reads back to a floor, so that month
   can be partial) and before the current month (still open). The list is read from the top without gaps, so a
   complete month without an EBI report is a published 0 in the total (no category row); a month outside the range
   has no row. Provenance: the newest version and the newest day document of the month (of the source, for a 0). #}
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
        cast(date_trunc('month', current_date) as date) as open_month,
        max(source_version) as source_version,
        arg_max(raw_sha256, ingested_at) as raw_sha256
    from reports
),

months as (
    select
        cast(unnest(generate_series(w.first_month, w.open_month - interval 1 month, interval 1 month)) as date)
            as date_key
    from window_months as w
    where w.first_month < w.open_month
),

counted as (
    select
        cast(date_trunc('month', r.published_date) as date) as date_key,
        coalesce(r.category, '_T') as category,
        count(*) as reports,
        max(r.source_version) as source_version,
        arg_max(r.raw_sha256, r.ingested_at) as raw_sha256
    from reports as r
    group by grouping sets ((date_trunc('month', r.published_date), r.category), (date_trunc('month', r.published_date)))
),

complete as (
    select
        m.date_key,
        coalesce(c.category, '_T') as category,
        coalesce(c.reports, 0) as reports,
        coalesce(c.source_version, w.source_version) as source_version,
        coalesce(c.raw_sha256, w.raw_sha256) as raw_sha256
    from months as m
    cross join window_months as w
    left join counted as c on m.date_key = c.date_key
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
from complete
