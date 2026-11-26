{# KNF monthly banking-sector data (register PLG-017): every value of the current workbook (bronze.knf_banking_cells,
   entity banking_monthly), newest Landing copy. Grain: entity x sheet_name x group_label x item_path x period_end.
   Layout (Dev probe 45758367857, workbook of 13 Nov 2026, data to September 2026): every sheet but 'Info' holds blocks;
   a block starts at a header row whose column 1 is empty and which holds at least two date cells (number format with
   'yy', e.g. yyyy-mm) from column 2 on; the block's bank group is the text in column 1 of the row right above the
   header ('Sektor bankowy', 'Banki komercyjne', 'Oddziały', 'Banki spółdzielcze'; 'Banki komercyjne bez
   zrzeszających' on the liquidity sheet). The block's rows run to the row above the next block's group row. Each row
   with text in column 1 is an item; its cells under the header's dates are its values (empty cells: no value, no
   row). Items are identified by their labels, never by row position:
   - a row with text and no value in any date column is a heading; consecutive headings form the context of the value
     rows that follow, until the next heading after a value row ('Zobowiązania wobec sektora niefinansowego > DEPOZYTY
     (bez depozytów zablokowanych)');
   - the leading spaces of column 1 give the item's level (0, 3, 6, ...): a value row's parent is the nearest value
     row above it in the same context with fewer leading spaces;
   - item_path = context and the parents' labels and the item's label, joined with ' > ' ('Aktywa, razem > Kasa,
     środki w bankach centralnych i inne depozyty płatne na żądanie').
   Values as stored (zł, counts, or fractions under a '%' number format); a value cell that is not a number keeps
   value null with cell_status 'unparsed', which fails accepted_values, so a new marker stops the build. frequency 'Q'
   when every date of the block is a quarter end, else 'M'. unit_label: the text below 'Dane w' at the top of the
   sheet ('zł', 'sztukach'). #}
with recursive cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        cell_type,
        value_text,
        number_format,
        try_cast(trim(value_text) as double) as number_value,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('knf_banking', 'knf_banking_cells')) }})
),

date_cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        cast(date '1899-12-30' + cast(round(number_value) as integer) as date) as period_end
    from cells
    where
        column_number >= 2
        and cell_type = 'n'
        and contains(lower(coalesce(number_format, '')), 'yy')
        and not contains(number_format, '#')
        and number_value between 1 and 2958465
),

column_one as (
    select
        entity,
        sheet_name,
        row_number
    from cells
    where column_number = 1 and trim(value_text) <> ''
),

header_rows as (
    select
        d.entity,
        d.sheet_name,
        d.row_number as header_row
    from date_cells as d
    left join column_one as o
        on d.entity = o.entity and d.sheet_name = o.sheet_name and d.row_number = o.row_number
    where o.row_number is null
    group by all
    having count(*) >= 2
),

blocks as (
    select
        h.entity,
        h.sheet_name,
        h.header_row,
        lead(h.header_row) over (partition by h.entity, h.sheet_name order by h.header_row) as next_header_row,
        trim(g.value_text) as group_label
    from header_rows as h
    left join cells as g
        on
            h.entity = g.entity and h.sheet_name = g.sheet_name
            and g.row_number = h.header_row - 1 and g.column_number = 1
),

block_dates as (
    select
        b.entity,
        b.sheet_name,
        b.header_row,
        d.column_number,
        d.period_end
    from blocks as b
    inner join date_cells as d
        on b.entity = d.entity and b.sheet_name = d.sheet_name and b.header_row = d.row_number
),

block_frequency as (
    select
        entity,
        sheet_name,
        header_row,
        case when bool_and(month(period_end) in (3, 6, 9, 12)) then 'Q' else 'M' end as frequency
    from block_dates
    group by all
),

labels as (
    select
        c.entity,
        c.sheet_name,
        b.header_row,
        b.group_label,
        c.row_number,
        trim(c.value_text) as item_label,
        length(c.value_text) - length(ltrim(c.value_text, ' ')) as indent
    from cells as c
    inner join blocks as b
        on
            c.entity = b.entity and c.sheet_name = b.sheet_name
            and c.row_number > b.header_row
            and (b.next_header_row is null or c.row_number < b.next_header_row - 1)
    where c.column_number = 1 and trim(c.value_text) <> ''
),

value_cells as (
    select
        l.entity,
        l.sheet_name,
        l.header_row,
        l.row_number,
        d.period_end,
        c.value_text,
        c.number_value,
        c.number_format,
        c.source_version,
        c.raw_sha256
    from labels as l
    inner join block_dates as d
        on l.entity = d.entity and l.sheet_name = d.sheet_name and l.header_row = d.header_row
    inner join cells as c
        on
            l.entity = c.entity and l.sheet_name = c.sheet_name and l.row_number = c.row_number
            and d.column_number = c.column_number
    where trim(c.value_text) <> ''
),

flagged as (
    select
        l.*,
        not exists (
            select 1
            from value_cells as v
            where v.entity = l.entity and v.sheet_name = l.sheet_name and v.row_number = l.row_number
        ) as is_heading
    from labels as l
),

runs as (
    select
        *,
        sum(
            case when is_heading and not coalesce(prev_heading, false) then 1 else 0 end
        ) over (partition by entity, sheet_name, header_row order by row_number) as context_id
    from (
        select
            *,
            lag(is_heading) over (partition by entity, sheet_name, header_row order by row_number) as prev_heading
        from flagged
    ) as f
),

contexts as (
    select
        entity,
        sheet_name,
        header_row,
        context_id,
        string_agg(item_label, ' > ' order by row_number) as context_path
    from runs
    where is_heading
    group by all
),

items as (
    select
        r.entity,
        r.sheet_name,
        r.header_row,
        r.group_label,
        r.context_id,
        r.row_number,
        r.item_label,
        r.indent,
        (
            select max(p.row_number)
            from runs as p
            where
                p.entity = r.entity and p.sheet_name = r.sheet_name and p.header_row = r.header_row
                and p.context_id = r.context_id and not p.is_heading
                and p.row_number < r.row_number and p.indent < r.indent
        ) as parent_row
    from runs as r
    where not r.is_heading
),

paths as (
    select
        entity,
        sheet_name,
        header_row,
        row_number,
        item_label as label_path
    from items
    where parent_row is null
    union all
    select
        i.entity,
        i.sheet_name,
        i.header_row,
        i.row_number,
        p.label_path || ' > ' || i.item_label as label_path
    from items as i
    inner join paths as p
        on
            i.entity = p.entity and i.sheet_name = p.sheet_name and i.header_row = p.header_row
            and i.parent_row = p.row_number
),

units as (
    select
        u.entity,
        u.sheet_name,
        trim(v.value_text) as unit_label
    from cells as u
    inner join cells as v
        on
            u.entity = v.entity and u.sheet_name = v.sheet_name
            and v.row_number = u.row_number + 1 and v.column_number = 1
    where u.column_number = 1 and trim(u.value_text) = 'Dane w'
)

select
    i.entity,
    i.sheet_name,
    i.group_label,
    case
        when x.context_path is null then p.label_path
        else x.context_path || ' > ' || p.label_path
    end as item_path,
    i.item_label,
    i.row_number as source_row,
    f.frequency,
    v.period_end,
    cast(date_trunc('month', v.period_end) as date) as period_date,
    v.number_value as value,
    case when v.number_value is null then 'unparsed' else 'ok' end as cell_status,
    v.value_text,
    v.number_format,
    un.unit_label,
    v.source_version,
    v.raw_sha256
from items as i
inner join paths as p
    on
        i.entity = p.entity and i.sheet_name = p.sheet_name and i.header_row = p.header_row
        and i.row_number = p.row_number
left join contexts as x
    on
        i.entity = x.entity and i.sheet_name = x.sheet_name and i.header_row = x.header_row
        and i.context_id = x.context_id
inner join block_frequency as f
    on i.entity = f.entity and i.sheet_name = f.sheet_name and i.header_row = f.header_row
inner join value_cells as v
    on i.entity = v.entity and i.sheet_name = v.sheet_name and i.row_number = v.row_number
left join units as un on i.entity = un.entity and i.sheet_name = un.sheet_name
