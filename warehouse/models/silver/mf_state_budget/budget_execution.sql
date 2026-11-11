{# Ministry of Finance monthly state budget execution reports (sprawozdanie operatywne), table 1 "Zestawienie
   ogólne z wykonania budżetu państwa", from the newest report of each year (entity budget_execution_<year>; each
   report repeats every month of its year so far, cumulative from January). One row per year × month × item ×
   measure: measure 'execution_ytd' (cumulative execution January to month) or 'plan' (the budget act for the
   year, month null). Thousand zloty, as published. Items keep the report's label (item_label) and numbering
   (item_code, e.g. 'I', '1.2'); item_key names the main lines (revenue, expenditure, balance, eu_funds_result,
   financing, financing_domestic, financing_foreign). A block starts at a row whose column 1 is 'Wyszczególnienie';
   a month header is a column whose text is a Roman range ('I', 'I - II', … 'I - XII') in that row or one of the
   next two (reports up to 2021 put the months a row lower). In 2018–2019 the domestic financing line carries its
   values on a continuation row, so financing_domestic is missing for those years. #}
with cells as (
    select
        *,
        cast(right(entity, 4) as integer) as budget_year,
        try_cast(value_text as double) as number_value,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text
    from ({{ xlsx_newest_cells(source('mf_state_budget', 'mf_state_budget_cells')) }})
    where regexp_matches(entity, '_[0-9]{4}$') and trim(sheet_name) = 'TABLICA 1'
),

header_rows as (
    select
        entity,
        row_number as header_row
    from cells
    where column_number = 1 and lower(text) = 'wyszczególnienie'
),

month_columns as (
    select
        c.entity,
        h.header_row,
        c.column_number,
        list_position(
            ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII'],
            regexp_extract(replace(c.text, ' ', ''), '^I(?:-([IVX]+))?$', 1)
        ) as month_end,
        regexp_matches(replace(c.text, ' ', ''), '^I(?:-[IVX]+)?$') as is_month
    from cells as c
    inner join header_rows as h
        on c.entity = h.entity and c.row_number between h.header_row and h.header_row + 2
    where c.column_number >= 3
),

months as (
    select
        entity,
        header_row,
        column_number,
        coalesce(nullif(month_end, 0), 1) as month_end
    from month_columns
    where is_month
),

labels as (
    select
        entity,
        budget_year,
        row_number,
        text as item_label,
        (
            select max(h.header_row) from header_rows as h
            where h.entity = c.entity and h.header_row < c.row_number
        ) as header_row
    from cells as c
    where column_number = 1 and text <> '' and not regexp_matches(text, '^[0-9]+$')
),

coded as (
    select
        *,
        regexp_extract(item_label, '^((?:[IVX]+a?\.)|(?:[0-9]+\.\s*)+)', 1) as numbering,
        trim(regexp_replace(item_label, '^((?:[IVX]+a?\.)|(?:[0-9]+\.\s*)+)', '')) as item_text
    from labels
    where header_row is not null
),

values_ as (
    select
        l.entity,
        l.budget_year,
        l.row_number,
        l.item_label,
        l.numbering,
        l.item_text,
        m.month_end,
        c.number_value
    from coded as l
    inner join months as m on l.entity = m.entity and l.header_row = m.header_row
    inner join cells as c
        on l.entity = c.entity and l.row_number = c.row_number and m.column_number = c.column_number
    where c.cell_type = 'n' and c.number_value is not null
),

plans as (
    select
        l.entity,
        l.budget_year,
        l.row_number,
        l.item_label,
        l.numbering,
        l.item_text,
        cast(null as integer) as month_end,
        c.number_value
    from coded as l
    inner join cells as c on l.entity = c.entity and l.row_number = c.row_number and c.column_number = 2
    where c.cell_type = 'n' and c.number_value is not null
    qualify row_number() over (partition by l.entity, l.item_label order by l.row_number) = 1
),

unioned as (
    select
        *,
        'execution_ytd' as measure
    from values_
    union all
    select
        *,
        'plan' as measure
    from plans
),

keyed as (
    select
        *,
        case
            when upper(item_text) = 'DOCHODY' then 'revenue'
            when upper(item_text) = 'WYDATKI' then 'expenditure'
            when upper(item_text) like 'DEFICYT%' or upper(item_text) like 'NADWYŻKA%' then 'balance'
            when upper(item_text) like 'WYNIK BUDŻETU ŚRODKÓW EUROPEJSKICH%' then 'eu_funds_result'
            when upper(item_text) like 'FINANSOWANIE%' then 'financing'
            when lower(item_text) = 'krajowe' then 'financing_domestic'
            when lower(item_text) = 'zagraniczne' then 'financing_foreign'
        end as item_key
    from unioned
),

newest as (
    select
        n.raw_sha256,
        n.source_version,
        n.ingested_at,
        n.entity
    from cells as n
    qualify row_number() over (partition by n.entity order by n.source_row_number) = 1
)

select
    k.budget_year,
    k.month_end,
    case when k.month_end is not null then make_date(k.budget_year, k.month_end, 1) end as period_month,
    k.measure,
    k.item_key,
    replace(rtrim(k.numbering, '. '), '. ', '.') as item_code,
    k.item_label,
    k.number_value as value_thousand_pln,
    n.raw_sha256,
    n.source_version,
    n.ingested_at
from keyed as k
inner join newest as n on k.entity = n.entity
-- a label listed twice for the same month (none today) keeps its first row
qualify row_number() over (
    partition by k.budget_year, k.measure, k.month_end, k.item_label order by k.row_number
) = 1
