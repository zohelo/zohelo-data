{# EPO patent statistics (register EX-004): counts of European patent applications and granted European patents from the
   EPO Technology Dashboard workbooks (bronze.epo_statistics_cells), every landed edition, newest copy of each workbook.
   Grain: edition x table_code x area_label x field_name (null = all fields) x period_year. Source: link.epo.org,
   linked from https://www.epo.org/en/about-us/statistics/data-download; entity = <table>_<edition>.
   Four layouts, every one read by header text, never by position:
   - applications_by_country, grants_by_country: header row whose column 1 is 'Country'; column 1 the ISO 3166-1
     alpha-2 code of the first-named applicant's country of residence as published ('Not classified', 'Total'), column 2
     its name, one column per year (header = the year).
   - applications_by_field, grants_by_field: header row whose column 1 is 'Field of technology' (a footnote digit may
     follow); column 1 the sector (first row of its group only, carried down), column 2 the field (WIPO IPC technology
     concordance), 'Unclassified' in column 1 alone; one column per year; all origins (area 'Total').
   - applications_by_field_country, grants_by_field_country: one sheet per year (the sheet name), the same field rows,
     one column per country of residence as named in the header ('Other countries', 'Total').
   - poland_regions: sheet 'Provinces' of the Poland workbook; header row whose column 1 starts with 'NUTS2'; column 1
     the NUTS 2 region as printed ('Total', 'Unclassified'), column 2 its English name; one column per year (the share
     and change columns are derived and not read).
   A data row is a row below the header with a label and at least one value cell. Every label x year is kept: a number
   -> 'ok'; a text cell that is a whole number -> that number, 'ok_text' (2025 edition: three '0' of grants_by_country
   2021, whose year still adds up to its total); an empty cell -> 'missing' with value null (never 0); any other text
   -> 'unexpected' (fails accepted_values). No suppression or no-value marker was seen in the 2025 edition.
   Values as published (counts). The year sheets of the field x country tables keep their own status dates (status_date,
   e.g. 2016: 27.1.2020): they are not restated with the edition. Checks of the reading: tests/epo_statistics_counts_complete.sql. #}
with cells as (
    select
        entity,
        regexp_replace(entity, '_[0-9]{4}$', '') as entity_code,
        cast(right(entity, 4) as integer) as edition,
        sheet_name,
        row_number,
        column_number,
        cell_type,
        value_text,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('epo_statistics', 'epo_statistics_cells')) }})
),

-- One file per entity (xlsx_newest_cells): its edition and provenance.
files as (
    select
        entity,
        edition,
        source_version,
        raw_sha256
    from cells
    group by all
),

sheets as (
    select
        entity,
        sheet_name,
        case
            when entity_code in ('applications_by_country', 'grants_by_country') then entity_code
            when entity_code in ('applications_by_field', 'grants_by_field') then entity_code
            when
                entity_code in ('applications_by_field_country', 'grants_by_field_country')
                and regexp_full_match(sheet_name, '[0-9]{4}') then entity_code
            when entity_code = 'poland' and sheet_name = 'Provinces' then 'poland_regions'
        end as table_code
    from cells
    group by all
),

headers as (
    select
        c.entity,
        c.sheet_name,
        s.table_code,
        min(c.row_number) as header_row
    from cells as c
    inner join sheets as s on c.entity = s.entity and c.sheet_name = s.sheet_name
    where
        c.column_number = 1
        and (
            (s.table_code like '%_by_country' and c.text = 'Country')
            or (s.table_code like '%_by_field%' and regexp_full_match(c.text, 'Field of technology[0-9]*'))
            or (s.table_code = 'poland_regions' and c.text like 'NUTS2%')
        )
    group by all
),

-- Value columns: a year in the header (country, field and region tables) or a country name (field x country tables).
value_columns as (
    select
        h.entity,
        h.sheet_name,
        h.table_code,
        h.header_row,
        c.column_number,
        case
            when h.table_code like '%_field_country' then cast(h.sheet_name as integer)
            else cast(try_cast(c.text as double) as integer)
        end as period_year,
        case when h.table_code like '%_field_country' then c.text end as column_area
    from headers as h
    inner join cells as c on h.entity = c.entity and h.sheet_name = c.sheet_name and h.header_row = c.row_number
    where
        c.column_number >= 3
        and c.text <> ''
        and (
            h.table_code like '%_field_country'
            or (
                regexp_full_match(c.text, '(19|20)[0-9]{2}')
                and try_cast(c.text as double) = floor(try_cast(c.text as double))
            )
        )
),

labels as (
    select
        h.entity,
        h.sheet_name,
        h.table_code,
        c.row_number,
        max(case when c.column_number = 1 then c.text end) as label_1,
        max(case when c.column_number = 2 then c.text end) as label_2
    from headers as h
    inner join cells as c on h.entity = c.entity and h.sheet_name = c.sheet_name and h.header_row < c.row_number
    where c.column_number in (1, 2) and c.text <> ''
    group by all
),

data_rows as (
    select l.*
    from labels as l
    where exists (
        select 1
        from value_columns as v
        inner join cells as c
            on v.entity = c.entity and v.sheet_name = c.sheet_name and v.column_number = c.column_number
        where v.entity = l.entity and v.sheet_name = l.sheet_name and c.row_number = l.row_number
    )
),

-- The sector of a field row: column 1 of the nearest row at or above it that has both a sector and a field.
sectors as (
    select
        r.entity,
        r.sheet_name,
        r.row_number,
        arg_max(s.label_1, s.row_number) as sector_name
    from data_rows as r
    inner join data_rows as s
        on
            r.entity = s.entity and r.sheet_name = s.sheet_name and r.row_number >= s.row_number
            and s.label_1 is not null and s.label_2 is not null
    where r.table_code like '%_by_field%' and r.label_2 is not null
    group by all
),

titles as (
    select
        h.entity,
        h.sheet_name,
        string_agg(c.text, ' | ' order by c.row_number) filter (where c.row_number < h.header_row) as table_title,
        max(regexp_extract(c.text, '^Status: ?(.+)$', 1))
        filter (where c.row_number > h.header_row and regexp_matches(c.text, '^Status:')) as status_date
    from headers as h
    inner join cells as c on h.entity = c.entity and h.sheet_name = c.sheet_name
    where c.column_number = 1 and c.text <> ''
    group by all
),

values_long as (
    select
        r.entity,
        r.sheet_name,
        r.table_code,
        r.row_number,
        case
            when r.table_code like '%_field_country' then v.column_area
            when r.table_code like '%_by_field' then 'Total'
            else r.label_1
        end as area_label,
        case
            when r.table_code like '%_by_country' and regexp_full_match(r.label_1, '[A-Z]{2}') then r.label_1
        end as area_code,
        case
            when r.table_code like '%_field_country' then v.column_area
            when r.table_code like '%_by_field' then 'Total'
            else coalesce(r.label_2, r.label_1)
        end as area_name,
        case when r.table_code like '%_by_field%' then s.sector_name end as sector_name,
        case when r.table_code like '%_by_field%' then coalesce(r.label_2, r.label_1) end as field_name,
        v.period_year,
        c.cell_type,
        c.value_text
    from data_rows as r
    inner join value_columns as v on r.entity = v.entity and r.sheet_name = v.sheet_name
    left join sectors as s on r.entity = s.entity and r.sheet_name = s.sheet_name and r.row_number = s.row_number
    left join cells as c
        on
            r.entity = c.entity and r.sheet_name = c.sheet_name and r.row_number = c.row_number
            and v.column_number = c.column_number
)

select
    l.entity,
    c.edition,
    l.table_code,
    case when l.table_code like 'grants%' then 'grants' else 'applications' end as indicator,
    case when l.table_code like 'grants%' then 'patents' else 'applications' end as unit_code,
    l.sheet_name,
    t.table_title,
    t.status_date,
    l.area_label,
    l.area_code,
    l.area_name,
    l.sector_name,
    l.field_name,
    l.period_year,
    case
        when l.cell_type = 'n' then try_cast(l.value_text as double)
        when regexp_full_match(trim(l.value_text), '[0-9]+') then try_cast(trim(l.value_text) as double)
    end as value,
    case
        when l.cell_type = 'n' and try_cast(l.value_text as double) is not null then 'ok'
        when regexp_full_match(trim(coalesce(l.value_text, '')), '[0-9]+') then 'ok_text'
        when trim(coalesce(l.value_text, '')) = '' then 'missing'
        else 'unexpected'
    end as cell_status,
    l.value_text as value_published,
    c.source_version,
    c.raw_sha256
from values_long as l
inner join files as c on l.entity = c.entity
left join titles as t on l.entity = t.entity and l.sheet_name = t.sheet_name
