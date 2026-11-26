{{ config(alias='build_xbrl_filings', tags=['gold_block']) }}
{# Gold build stage: counts of the ESEF annual financial reports of Polish issuers that filings.xbrl.org (XBRL
   International) holds, by calendar year of the report's period end (frequency A, geography PL). Block
   'esef_reports', two measures with their own keys: filings (every filing: language versions and refilings of
   one report each count) and issuers (distinct LEI, so those do not inflate it). The repository says it is not
   complete: these count what it holds, not every report filed in Poland. A year without filings has no row.
   Source: silver.xbrl_filings_filings (one list file: its version and SHA-256). #}
with yearly as (
    select
        year(period_end) as period_year,
        count(*) as filings,
        count(distinct lei) as issuers,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from {{ ref('xbrl_filings_filings') }}
    where country_code = 'PL'
    group by year(period_end)
),

measures as (
    select
        period_year,
        'filings' as unit_code,
        'filings' as unit_name,
        'xbrl_filings:esef_reports:filings' as measure_key,
        'ESEF annual financial reports held by filings.xbrl.org, by year of period end (filings; language '
        || 'versions and refilings each count)' as measure_name,
        'filings' as measure_codes,
        cast(filings as double) as value,
        source_version,
        raw_sha256
    from yearly
    union all
    select
        period_year,
        'entities' as unit_code,
        'legal entities' as unit_name,
        'xbrl_filings:esef_reports:entities:issuers' as measure_key,
        'Issuers with an ESEF annual financial report held by filings.xbrl.org, by year of period end '
        || '(distinct LEI)' as measure_name,
        'issuers' as measure_codes,
        cast(issuers as double) as value,
        source_version,
        raw_sha256
    from yearly
)

select
    'xbrl_filings' as source_system,
    'esef_reports' as block_id,
    'ESEF annual financial reports held by filings.xbrl.org' as block_name,
    measure_key,
    measure_name,
    measure_codes,
    unit_code,
    unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'A' as frequency,
    make_date(period_year, 1, 1) as date_key,
    cast(period_year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from measures
