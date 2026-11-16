{{ config(alias='build_knf_espi', tags=['gold_block']) }}
{# Gold build stage: ESPI issuer reports received by KNF (silver.knf_espi_reports) counted per month of receipt,
   Poland. Fact kind: event counts. measure_key knf_espi:reports:reports, monthly only; breakdown report_type
   (the ESPI form code as listed, '_U' none) and the total (report_type _T, is_total). Only complete months: after
   the month of the oldest day held (the history is read newest first, so that month can be partial) and before the
   current month (still open); a month outside that range has no row, never a 0. A report is counted once, on its
   day of receipt; a correction is its own report. Provenance: the newest version and the newest day document of
   the month. #}
with reports as (
    select
        received_date,
        report_type,
        source_version,
        raw_sha256,
        ingested_at
    from {{ ref('knf_espi_reports') }}
),

window_months as (
    select
        date_trunc('month', min(received_date)) + interval 1 month as first_month,
        date_trunc('month', current_date) as open_month
    from reports
),

counted as (
    select
        cast(date_trunc('month', r.received_date) as date) as date_key,
        coalesce(r.report_type, '_T') as report_type,
        count(*) as reports,
        max(r.source_version) as source_version,
        arg_max(r.raw_sha256, r.ingested_at) as raw_sha256
    from reports as r
    cross join window_months as w
    where r.received_date >= w.first_month and r.received_date < w.open_month
    group by grouping sets ((date_trunc('month', r.received_date), r.report_type), (date_trunc('month', r.received_date)))
)

select
    'knf_espi' as source_system,
    'reports' as block_id,
    'ESPI issuer reports received by KNF (OAM list)' as block_name,
    'knf_espi:reports:reports' as measure_key,
    'ESPI issuer reports received by KNF per month' as measure_name,
    'reports' as measure_codes,
    'reports' as unit_code,
    'reports' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'M' as frequency,
    date_key,
    strftime(date_key, '%Y-%m') as period_label,
    cast(json_object('report_type', report_type) as varchar) as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    report_type = '_T' as is_total,
    cast(reports as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from counted
