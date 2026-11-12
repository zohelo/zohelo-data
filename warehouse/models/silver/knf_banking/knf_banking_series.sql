{# KNF monthly banking-sector data (register PLG-017): every table sheet of the newest copy of the current workbook
   ("Dane miesięczne sektora bankowego", January 2018 on) and of the archive workbook (to December 2017, before the
   IFRS change) in long form. Source: bronze.knf_banking_cells (ingestion/common/xlsx_cells.py).
   Grain: entity × sheet × source row × period end (one row per item row and header date of a block).

   Layout (Dev trial 43285080215): a sheet holds one block per bank group; a block starts at a header row (at least
   three date cells from column 2 on: month or quarter ends) and its bank group is the text in column 1 of the row
   above it ("Sektor bankowy", "Banki komercyjne", "Oddziały", "Banki spółdzielcze"; archive "Sektor bankowy z
   oddziałami zagranicznymi", ...). Every column-1 row of a block with at least one cell under a header date is an
   item; a column-1 row without such cells is a section heading ("SEKTOR NIEFINANSOWY", "LCR", "NSFR"), kept as
   section_label of the items below it. Values as stored: amounts in zloty ("Dane w zł"), counts ("Dane w
   sztukach"), ratios as fractions ('%'-formatted cells, 0.1886 = 18.86 %); no rescaling. A text cell under a header
   date (a marker such as "-", "x", "b.d.") is kept with value null and value_marker set, never as 0; an empty cell
   gives no row. frequency: Q for a block whose header dates are all quarter ends, and for an item of a monthly
   block whose values all fall on quarter ends (NSFR in "Płynność"); M otherwise. #}
with cells as (
    select
        *,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text,
        try_cast(value_text as double) as number_value,
        cell_type = 'n'
        and number_format is not null
        and regexp_matches(lower(number_format), 'y')
        and not contains(number_format, '#') as is_date
    from ({{ xlsx_newest_cells(source('knf_banking', 'knf_banking_cells')) }}) as newest_cells
    where sheet_name not in ('Info', 'SPIS TABEL')
),

date_cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        cast(date '1899-12-30' + cast(round(number_value) as integer) as date) as period_end
    from cells
    where is_date and column_number >= 2 and number_value between 1 and 2958465
),

header_rows as (
    select
        entity,
        sheet_name,
        row_number as header_row,
        bool_and(month(period_end) in (3, 6, 9, 12)) as quarterly
    from date_cells
    group by all
    having count(*) >= 3
),

headers as (
    select
        d.entity,
        d.sheet_name,
        h.header_row,
        h.quarterly,
        d.column_number,
        d.period_end
    from date_cells as d
    inner join header_rows as h
        on d.entity = h.entity and d.sheet_name = h.sheet_name and d.row_number = h.header_row
),

labels as (
    select
        c.entity,
        c.sheet_name,
        c.row_number,
        c.text as label,
        (
            select max(h.header_row)
            from header_rows as h
            where h.entity = c.entity and h.sheet_name = c.sheet_name and h.header_row < c.row_number
        ) as header_row
    from cells as c
    where c.column_number = 1 and c.cell_type <> 'n' and c.text <> ''
),

-- the sheet's printed unit: "Dane w" in A3 and the unit in A4 (current workbook); the archive prints none
sheet_units as (
    select
        u.entity,
        u.sheet_name,
        u.text as unit_text
    from cells as u
    inner join cells as d
        on u.entity = d.entity and u.sheet_name = d.sheet_name and d.row_number = 3 and d.column_number = 1
    where u.row_number = 4 and u.column_number = 1 and d.text = 'Dane w'
),

grid as (
    select
        l.entity,
        l.sheet_name,
        l.header_row,
        h.quarterly,
        l.row_number as source_row,
        l.label as item_label,
        h.period_end,
        c.cell_type,
        c.value_text,
        c.number_value,
        c.number_format,
        c.raw_sha256,
        c.source_version,
        c.ingested_at
    from labels as l
    inner join headers as h
        on l.entity = h.entity and l.sheet_name = h.sheet_name and l.header_row = h.header_row
    inner join cells as c
        on l.entity = c.entity and l.sheet_name = c.sheet_name and l.row_number = c.row_number
        and h.column_number = c.column_number
),

items as (
    select
        entity,
        sheet_name,
        header_row,
        source_row,
        item_label,
        count(*) filter (where cell_type = 'n') as numbers,
        bool_and(month(period_end) in (3, 6, 9, 12)) filter (where cell_type = 'n') as values_on_quarter_ends
    from grid
    group by all
),

placed as (
    select
        i.*,
        (
            select arg_max(l.label, l.row_number)
            from labels as l
            where l.entity = i.entity and l.sheet_name = i.sheet_name and l.row_number < i.header_row
        ) as bank_group,
        (
            select arg_max(l.label, l.row_number)
            from labels as l
            where
                l.entity = i.entity and l.sheet_name = i.sheet_name and l.header_row = i.header_row
                and l.row_number < i.source_row
                and not exists (
                    select 1 from items as i2
                    where i2.entity = l.entity and i2.sheet_name = l.sheet_name and i2.source_row = l.row_number
                )
        ) as section_label
    from items as i
),

numbered as (
    select
        *,
        row_number() over (
            partition by entity, sheet_name, header_row, coalesce(section_label, ''), item_label order by source_row
        ) as item_occurrence
    from placed
)

select
    g.entity,
    g.sheet_name,
    n.bank_group,
    case
        when n.bank_group in ('Sektor bankowy', 'Sektor bankowy z oddziałami zagranicznymi', 'Dane strukturalne')
            then 'sector'
        when n.bank_group = 'Banki komercyjne' or n.bank_group like 'Krajowe banki komercyjne%' then 'commercial'
        when n.bank_group = 'Banki komercyjne bez zrzeszających' then 'commercial_excl_affiliating'
        when n.bank_group in ('Oddziały', 'Oddziały instytucji kredytowych') then 'branches'
        when n.bank_group like 'Banki spółdzielcze%' then 'cooperative'
        when n.bank_group like 'Kalkulacja miar płynności przez bank o sumie bilansowej powyżej 200 mln PLN%'
            then 'assets_over_200m_pln'
        when n.bank_group like 'Kalkulacja miar płynności przez bank o sumie bilansowej do 200 mln PLN%'
            then 'assets_upto_200m_pln'
    end as bank_group_code,
    n.section_label,
    g.item_label,
    n.item_occurrence,
    case when g.quarterly or (n.numbers >= 2 and n.values_on_quarter_ends) then 'Q' else 'M' end as frequency,
    g.period_end,
    case
        when g.quarterly or (n.numbers >= 2 and n.values_on_quarter_ends) then date_trunc('quarter', g.period_end)
        else date_trunc('month', g.period_end)
    end::date as period_start,
    case when g.cell_type = 'n' then g.number_value end as value,
    case when g.cell_type <> 'n' then g.value_text end as value_marker,
    g.number_format as value_format,
    u.unit_text,
    case
        when contains(coalesce(g.number_format, ''), '%') then 'ratio'
        when u.unit_text = 'zł' then 'pln'
        when u.unit_text = 'sztukach' then 'units'
        when u.unit_text is null then 'unstated'
    end as unit_code,
    g.header_row,
    g.source_row,
    g.raw_sha256,
    g.source_version,
    g.ingested_at
from grid as g
inner join numbered as n
    on g.entity = n.entity and g.sheet_name = n.sheet_name and g.source_row = n.source_row
left join sheet_units as u on g.entity = u.entity and g.sheet_name = u.sheet_name
