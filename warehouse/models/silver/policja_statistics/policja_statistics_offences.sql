{# Polish Police statistics portal (register PLG-033): offences recorded, offences detected and the detection rate per
   offence category, area and year, from the newest copy of each category workbook (bronze.policja_statistics_cells).
   Grain: category_code x area_name x measure_code x period_year. Source: statystyka.policja.pl, workbook
   "<category> wg jednostek podziału administracyjnego kraju" (one sheet; areas as printed: Poland, the 16
   voivodeships and the two police areas inside Mazowieckie; years 1999 to the end year in the file name).
   Columns are found by their header text in the row whose column 1 is "Jednostka podziału administracyjnego", never by
   position; a data row is a row under that header whose year cell is a whole number. Every area x year x measure is
   kept: 'n.d.' (no data) -> cell_status not_available, an empty or blank cell -> missing, both with value null (never
   0); any other text -> unexpected (fails accepted_values). Values as published: offences (counts) and % detected
   (detected offences can exceed recorded ones: they include offences recorded in earlier years). #}
with cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        value_text,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('policja_statistics', 'policja_statistics_cells')) }})
),

headers as (
    select
        entity,
        sheet_name,
        row_number as header_row
    from cells
    where column_number = 1 and text = 'Jednostka podziału administracyjnego'
),

header_columns as (
    select
        c.entity,
        c.sheet_name,
        c.column_number,
        case c.text
            when 'Rok' then 'year'
            when 'Przestępstwa stwierdzone' then 'recorded'
            when 'Przestępstwa wykryte' then 'detected'
            when '% wykrycia' then 'detection_rate'
        end as role
    from cells as c
    inner join headers as h on c.entity = h.entity and c.sheet_name = h.sheet_name and c.row_number = h.header_row
    where c.column_number > 1
),

titles as (
    select
        c.entity,
        c.sheet_name,
        arg_min(c.text, c.row_number) as table_title
    from cells as c
    inner join headers as h on c.entity = h.entity and c.sheet_name = h.sheet_name and c.row_number < h.header_row
    where c.column_number = 1 and c.text <> ''
    group by all
),

data_rows as (
    select
        a.entity,
        a.sheet_name,
        a.row_number,
        a.text as area_name,
        cast(try_cast(y.value_text as double) as integer) as period_year,
        a.source_version,
        a.raw_sha256
    from cells as a
    inner join headers as h on a.entity = h.entity and a.sheet_name = h.sheet_name and a.row_number > h.header_row
    inner join header_columns as hy on a.entity = hy.entity and a.sheet_name = hy.sheet_name and hy.role = 'year'
    inner join cells as y
        on
            a.entity = y.entity and a.sheet_name = y.sheet_name and a.row_number = y.row_number
            and hy.column_number = y.column_number
    where
        a.column_number = 1
        and a.text <> ''
        and try_cast(y.value_text as double) between 1900 and 2100
        and try_cast(y.value_text as double) = floor(try_cast(y.value_text as double))
),

values_long as (
    select
        r.entity,
        r.sheet_name,
        r.area_name,
        r.period_year,
        hc.role as measure_code,
        v.value_text,
        try_cast(v.value_text as double) as number_value,
        r.source_version,
        r.raw_sha256
    from data_rows as r
    inner join header_columns as hc
        on
            r.entity = hc.entity and r.sheet_name = hc.sheet_name
            and hc.role in ('recorded', 'detected', 'detection_rate')
    left join cells as v
        on
            r.entity = v.entity and r.sheet_name = v.sheet_name and r.row_number = v.row_number
            and hc.column_number = v.column_number
)

select
    l.entity as category_code,
    t.table_title,
    l.sheet_name,
    l.area_name,
    l.measure_code,
    case when l.measure_code = 'detection_rate' then 'pct' else 'offences' end as unit_code,
    l.period_year,
    l.number_value as value,
    case
        when l.number_value is not null then 'ok'
        when trim(coalesce(l.value_text, '')) = '' then 'missing'
        when trim(l.value_text) = 'n.d.' then 'not_available'
        else 'unexpected'
    end as cell_status,
    l.value_text as value_published,
    l.source_version,
    l.raw_sha256
from values_long as l
left join titles as t on l.entity = t.entity and l.sheet_name = t.sheet_name
