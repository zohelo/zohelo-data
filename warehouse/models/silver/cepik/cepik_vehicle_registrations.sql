{# CEPiK statistics workbooks (register PL-ENV-024): vehicles registered per month, by voivodeship (with the national
   total row = 'polska') and by vehicle type (Poland), from the newest copy of each workbook (bronze.cepik_cells;
   entity vehicles_voivodeship_<year> / vehicles_type_<year>). Grain: table_code x group_code x period_year x
   period_month.
   Layout (Dev probe 43340837086, 2016-2026, one sheet per workbook; the sheet name, e.g. WOJEWODZTWA_2016, does not
   tell the topic, the entity does): blocks of three months; a block starts at a header row whose column 1 is
   'WOJEWÓDZTWO' (voivodeship workbook) or 'RODZAJ' (vehicle type workbook) and which names the months in Polish
   (styczeń ... grudzień); the rows below it until the next header with a text label in column 1 are the data rows.
   A value belongs to the month whose header cell is in its column or one or two columns left of it (2022 prints
   March under column K, its header in J); a number in a data row outside every month's columns keeps period_month
   null and fails not_null, a text value keeps value null with cell_status 'unparsed' and fails accepted_values: an
   unknown layout stops the build instead of being read by position. The national total is the row right below the
   Roman month header (I ... XII); it is kept only for months that have voivodeship values in the same workbook
   (the current year's workbook prints 0 for months not published yet), so an unpublished month is absent, never 0.
   Blank cells (no registrations printed) are absent. Label typo 'WARMIKŃSKO-MAZURSKIE' (2016-2019) read as
   warmińsko-mazurskie.
   check_status: 'order_suspect' for every voivodeship row of a month in which any voivodeship's share of the
   month's voivodeship sum differs from its median share (all months) by a factor above 1.5 (the 2026 workbook of
   22 Oct 2026 prints September's values in another order than the names); Gold leaves those rows out, the
   national total of the month stays. #}
with cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        trim(value_text) as value_text,
        try_cast(trim(value_text) as double) as number_value,
        source_version,
        raw_sha256,
        case
            when starts_with(entity, 'vehicles_voivodeship_') then 'voivodeship'
            when starts_with(entity, 'vehicles_type_') then 'vehicle_type'
        end as table_code,
        try_cast(right(entity, 4) as integer) as period_year
    from ({{ xlsx_newest_cells(source('cepik', 'cepik_cells')) }})
),

month_names (month_name, period_month) as (
    values
    ('styczeń', 1), ('luty', 2), ('marzec', 3), ('kwiecień', 4), ('maj', 5), ('czerwiec', 6),
    ('lipiec', 7), ('sierpień', 8), ('wrzesień', 9), ('październik', 10), ('listopad', 11), ('grudzień', 12)
),

roman (numeral, period_month) as (
    values
    ('I', 1), ('II', 2), ('III', 3), ('IV', 4), ('V', 5), ('VI', 6),
    ('VII', 7), ('VIII', 8), ('IX', 9), ('X', 10), ('XI', 11), ('XII', 12)
),

header_rows as (
    select
        entity,
        sheet_name,
        row_number as header_row,
        lead(row_number) over (partition by entity, sheet_name order by row_number) as next_header_row
    from cells
    where
        column_number = 1
        and (
            (table_code = 'voivodeship' and upper(value_text) = 'WOJEWÓDZTWO')
            or (table_code = 'vehicle_type' and upper(value_text) = 'RODZAJ')
        )
),

month_columns as (
    select
        c.entity,
        c.sheet_name,
        h.header_row,
        c.column_number as header_column,
        m.period_month
    from cells as c
    inner join header_rows as h
        on c.entity = h.entity and c.sheet_name = h.sheet_name and c.row_number = h.header_row
    inner join month_names as m on lower(c.value_text) = m.month_name
),

data_rows as (
    select
        c.entity,
        c.sheet_name,
        c.row_number,
        h.header_row,
        c.table_code,
        c.period_year,
        c.value_text as group_label
    from cells as c
    inner join header_rows as h
        on
            c.entity = h.entity
            and c.sheet_name = h.sheet_name
            and c.row_number > h.header_row
            and (h.next_header_row is null or c.row_number < h.next_header_row)
    where c.column_number = 1 and c.number_value is null and c.value_text <> ''
),

block_values as (
    select
        r.table_code,
        r.period_year,
        r.group_label,
        r.entity,
        c.value_text,
        c.number_value,
        c.source_version,
        c.raw_sha256,
        m.period_month
    from data_rows as r
    inner join cells as c
        on c.entity = r.entity and c.sheet_name = r.sheet_name and c.row_number = r.row_number and c.column_number > 1
    left join month_columns as m
        on
            m.entity = r.entity
            and m.sheet_name = r.sheet_name
            and m.header_row = r.header_row
            and c.column_number between m.header_column and m.header_column + 2
),

roman_rows as (
    select
        entity,
        sheet_name,
        row_number as roman_row
    from cells
    where column_number = 1 and value_text = 'I'
),

national as (
    select
        c.table_code,
        c.period_year,
        'POLSKA' as group_label,
        c.entity,
        c.value_text,
        c.number_value,
        c.source_version,
        c.raw_sha256,
        rm.period_month
    from roman_rows as rr
    inner join cells as h on h.entity = rr.entity and h.sheet_name = rr.sheet_name and h.row_number = rr.roman_row
    inner join roman as rm on h.value_text = rm.numeral
    inner join cells as c
        on
            c.entity = rr.entity
            and c.sheet_name = rr.sheet_name
            and c.row_number = rr.roman_row + 1
            and c.column_number = h.column_number
    where c.table_code = 'voivodeship'
),

published_months as (
    select distinct
        entity,
        period_month
    from block_values
    where table_code = 'voivodeship' and number_value is not null
),

all_values as (
    select * from block_values
    union all
    select n.*
    from national as n
    inner join published_months as p on n.entity = p.entity and n.period_month = p.period_month
),

typed as (
    select
        table_code,
        case
            when lower(group_label) = 'warmikńsko-mazurskie' then 'warmińsko-mazurskie'
            when table_code = 'voivodeship' and upper(group_label) = 'POLSKA' then 'polska'
            else lower(group_label)
        end as group_code,
        group_label,
        period_year,
        period_month,
        number_value as value,
        case when number_value is null then 'unparsed' else 'ok' end as cell_status,
        value_text,
        entity,
        source_version,
        raw_sha256
    from all_values
),

regions as (
    select
        *,
        value / sum(value) over (partition by entity, period_month) as month_share
    from typed
    where table_code = 'voivodeship' and group_code <> 'polska' and value is not null
),

shares as (
    select
        *,
        month_share / median(month_share) over (partition by group_code) as share_ratio
    from regions
),

suspect_months as (
    select
        entity,
        period_month
    from shares
    group by all
    having max(greatest(share_ratio, 1 / nullif(share_ratio, 0))) > 1.5
)

select
    t.table_code,
    t.group_code,
    t.group_label,
    t.period_year,
    t.period_month,
    make_date(t.period_year, t.period_month, 1) as period_date,
    t.value,
    t.cell_status,
    case
        when t.table_code = 'voivodeship' and t.group_code <> 'polska' and s.entity is not null then 'order_suspect'
        else 'ok'
    end as check_status,
    t.value_text,
    t.entity,
    t.source_version,
    t.raw_sha256
from typed as t
left join suspect_months as s on t.entity = s.entity and t.period_month = s.period_month
