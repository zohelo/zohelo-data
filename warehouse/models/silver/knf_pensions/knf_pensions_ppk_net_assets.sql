{# KNF quarterly PPK information (register PLG-018): net asset value of the PPK target-date funds by managing
   institution, and the total, from 'Tabela 2' of the newest release (bronze.knf_pensions_cells, entity ppk_quarterly).
   Grain: entity x manager_label x period_year x period_quarter.
   Layout (Dev probe 45758367857, release of 30 Oct 2026, 2020 Q1 to 2026 Q3): the sheet's title row (column 2 starts
   'Tabela 2:') names the unit ('(w tys. PLN, stan na koniec okresu)'); the table's header is the first row whose
   column 2 is 'Zarządzający': it holds the year in the first column of each year (merged over the year's quarters),
   the row below it the quarter ('1Q' ... '4Q') in every value column. A column's year is the nearest year cell at or
   left of it on the header row; the column must carry a quarter label, and the year's own column must be '1Q' (test
   knf_pensions_ppk_net_assets_complete: every quarter from the first to the last, once). Item rows run from the row
   below the quarter row to the row whose column 2 is 'Suma' (the total, is_total); the second table of the sheet
   (shares of the managers, under a second 'Zarządzający' header) is not read. Cells: a number is the value as
   published (thousand PLN); '-' (no fund yet, or none left) keeps value null with cell_status 'no_value', never 0;
   any other text keeps value null with cell_status 'unparsed', which fails accepted_values. #}
with cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        trim(value_text) as value_text,
        try_cast(trim(value_text) as double) as number_value,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('knf_pensions', 'knf_pensions_cells')) }})
    where sheet_name = 'Tabela 2'
),

title as (
    select
        entity,
        regexp_extract(value_text, '\(w ([^,)]+)', 1) as unit_label
    from cells
    where column_number = 2 and starts_with(value_text, 'Tabela 2:')
),

header as (
    select
        entity,
        min(row_number) as header_row
    from cells
    where column_number = 2 and value_text = 'Zarządzający'
    group by all
),

total_row as (
    select
        c.entity,
        min(c.row_number) as suma_row
    from cells as c
    inner join header as h on c.entity = h.entity and c.row_number > h.header_row
    where c.column_number = 2 and c.value_text = 'Suma'
    group by all
),

years as (
    select
        c.entity,
        c.column_number,
        cast(c.value_text as integer) as period_year
    from cells as c
    inner join header as h on c.entity = h.entity and c.row_number = h.header_row
    where c.column_number >= 3 and regexp_full_match(c.value_text, '20[0-9]{2}')
),

quarters as (
    select
        c.entity,
        c.column_number,
        cast(left(c.value_text, 1) as integer) as period_quarter
    from cells as c
    inner join header as h on c.entity = h.entity and c.row_number = h.header_row + 1
    where c.column_number >= 3 and regexp_full_match(c.value_text, '[1-4]Q')
),

columns as (
    select
        q.entity,
        q.column_number,
        q.period_quarter,
        (
            select arg_max(y.period_year, y.column_number)
            from years as y
            where y.entity = q.entity and y.column_number <= q.column_number
        ) as period_year
    from quarters as q
),

items as (
    select
        c.entity,
        c.row_number,
        c.value_text as manager_label,
        c.row_number = t.suma_row as is_total
    from cells as c
    inner join header as h on c.entity = h.entity
    inner join total_row as t on c.entity = t.entity
    where c.column_number = 2 and c.row_number > h.header_row + 1 and c.row_number <= t.suma_row and c.value_text <> ''
)

select
    i.entity,
    i.manager_label,
    i.is_total,
    k.period_year,
    k.period_quarter,
    cast(make_date(k.period_year, k.period_quarter * 3, 1) + interval 1 month - interval 1 day as date) as period_end,
    v.number_value as value,
    case
        when v.number_value is not null then 'ok'
        when v.value_text = '-' then 'no_value'
        else 'unparsed'
    end as cell_status,
    v.value_text,
    u.unit_label,
    i.row_number as source_row,
    v.source_version,
    v.raw_sha256
from items as i
inner join cells as v on i.entity = v.entity and i.row_number = v.row_number
inner join columns as k on v.entity = k.entity and v.column_number = k.column_number
left join title as u on i.entity = u.entity
