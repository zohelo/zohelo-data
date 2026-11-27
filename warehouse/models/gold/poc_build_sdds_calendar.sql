{{ config(alias='build_sdds_calendar', tags=['gold_block']) }}
{# Gold build stage: SDDS advance release calendar of Poland, NBP (register TP-012) and Ministry of Finance (TP-013)
   releases, from gold.sdds_release (rows with check_status 'ok'; the flagged slips are not counted). Counts of
   expected releases per month of the release date (frequency M, date_key = the month's first day), Poland. Block
   releases, unit releases, one measure_key per breakdown:
   - sdds_calendar:releases:releases:total (NBP and Ministry of Finance together, is_total);
   - ...:publisher ({"publisher": "nbp" | "mf"}, the publisher's name in breakdown_label_json).
   A month without a listed release has no row (never a 0). observation_status_key: 'ok' for a month that had ended
   on the download date of the workbook read, 'provisional' otherwise (the dates are expected and may change). #}
with releases as (
    select
        *,
        cast(date_trunc('month', release_date) as date) as month_start
    from {{ ref('sdds_release') }}
    where check_status = 'ok'
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'sdds_calendar'
),

counted as (
    select
        'total' as breakdown_code,
        month_start,
        '{}' as breakdown_json,
        cast(null as varchar) as breakdown_label_json,
        count(*) as releases,
        max(copy_date) as copy_date,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from releases
    group by month_start
    union all
    select
        'publisher' as breakdown_code,
        month_start,
        cast(json_object('publisher', publisher) as varchar) as breakdown_json,
        cast(json_object(
            'publisher', case publisher when 'nbp' then 'Narodowy Bank Polski' else 'Ministerstwo Finansów' end
        ) as varchar) as breakdown_label_json,
        count(*) as releases,
        max(copy_date) as copy_date,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from releases
    group by month_start, publisher
)

select
    'sdds_calendar' as source_system,
    'releases' as block_id,
    'SDDS advance release calendar: NBP and Ministry of Finance' as block_name,
    'sdds_calendar:releases:releases:' || c.breakdown_code as measure_key,
    case c.breakdown_code
        when 'total' then 'Expected releases of NBP and Ministry of Finance statistics (SDDS calendar)'
        else 'Expected releases of NBP and Ministry of Finance statistics (SDDS calendar), by publisher'
    end as measure_name,
    c.breakdown_code as measure_codes,
    'releases' as unit_code,
    'releases' as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    c.month_start as date_key,
    strftime(c.month_start, '%Y-%m') as period_label,
    c.breakdown_json,
    c.breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.breakdown_code = 'total' as is_total,
    cast(c.releases as double) as value,
    case when last_day(c.month_start) <= c.copy_date then 'ok' else 'provisional' end as observation_status_key,
    c.source_version,
    c.raw_sha256
from counted as c
left join geo_map as g on g.source_code = 'PL'
