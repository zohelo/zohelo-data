{{ config(alias='sdds_release') }}
{# Gold entity table (release calendar events, docs/model.md "Fact kinds"): one row per expected release of the NBP
   (register TP-012) and the Ministry of Finance (TP-013) in the SDDS advance release calendar of Poland, from
   silver.sdds_calendar_releases, keyed by category x reference_period x release_date. Every release year is read
   from one workbook: the calendar of that year when Landing holds it, else the newest calendar listing the year (the
   2026 workbook lists January and February 2027 for the Ministry of Finance): a choice between versions, by rule.
   GUS rows of the workbook are left out (the GUS kalendarium of release_calendars covers GUS releases). check_status
   is kept: the build stage counts only 'ok' rows. Every date is the producer's expected date ("Wszystkie daty są
   oczekiwanymi datami publikacji i mogą ulec zmianie"); copy_date is the download date of the workbook read. #}
with releases as (
    select
        *,
        year(release_date) as release_year
    from {{ ref('sdds_calendar_releases') }}
    where publisher in ('nbp', 'mf')
),

chosen as (
    select
        release_year,
        calendar_year
    from releases
    group by release_year, calendar_year
    qualify rank() over (
        partition by release_year order by (calendar_year = release_year) desc, calendar_year desc
    ) = 1
)

select
    r.category || '|' || r.reference_period || '|' || r.release_date as release_key,
    r.release_date,
    r.publisher,
    r.sector,
    r.category,
    r.reference_period,
    r.period_frequency,
    r.period_start,
    r.period_end,
    r.column_month,
    r.last_update,
    r.check_status,
    r.calendar_year,
    cast(r.ingested_at as date) as copy_date,
    'sdds_calendar' as source_system,
    r.source_version,
    r.raw_sha256
from releases as r
inner join chosen as c on r.release_year = c.release_year and r.calendar_year = c.calendar_year
