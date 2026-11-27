{# UPRP industrial-property statistics (register EX-001): the statistics tables of the Polish annual reports
   (uprp.gov.pl PDF files, cells read and checked in Bronze, bronze.uprp_statistics_cells). Counts a year in Poland:
   applications filed and rights granted or registered for inventions (patents), utility models (protection rights),
   trademarks (protection rights) and industrial designs (registration rights), by procedure (national; PCT or the
   international Madrid and Hague routes) and origin of the applicant (domestic, foreign) as the tables print them,
   and European patents validated in Poland (total only). Only cells whose guard starts with 'ok' (a sum of their
   table holds, or for a validations total of the report's own year its labelled figure,
   ingestion/to_bronze/uprp_statistics/) are kept. Each report prints the five years up to its own; the value of the
   newest report holding a checked cell wins (ranked by report year, so two reports of one year with one key fail
   the unique test instead of being picked from). Values as printed (digits grouped by spaces).
   Grain: table_code x row_code x year. #}
with cells as (
    select
        entity,
        table_code,
        table_title,
        page_number,
        row_code,
        row_label,
        cast(year_text as integer) as year,
        cast(replace(value_text, ' ', '') as bigint) as value,
        value_text,
        cast(regexp_extract(entity, '^raport_(\d{4})$', 1) as integer) as report_year,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('uprp_statistics', 'uprp_statistics_cells')) }})
    where starts_with(guard, 'ok')
)

select
    table_code,
    row_code,
    year,
    split_part(table_code, '_', -1) as event_code,
    regexp_replace(table_code, '_(applications|grants|validations)$', '') as right_code,
    case
        when row_code in ('national', 'national_domestic', 'national_foreign') then 'national'
        when row_code in ('international', 'international_domestic', 'international_foreign') then 'international'
        else '_T'
    end as procedure_code,
    case
        when row_code like '%domestic' then 'domestic'
        when row_code like '%foreign' then 'foreign'
        else '_T'
    end as origin_code,
    value,
    value_text,
    report_year,
    table_title,
    row_label,
    page_number,
    entity,
    source_version,
    raw_sha256
from cells
qualify rank() over (partition by table_code, row_code, year order by report_year desc) = 1
