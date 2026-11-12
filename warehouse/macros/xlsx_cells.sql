{# Workbook sources (bronze.<source>_cells, ingestion/common/xlsx_cells.py): helpers for Silver.

   xlsx_newest_cells(relation): the cells of the newest Landing copy of each entity (one row per cell;
   a cell loaded twice from the same file is kept once).

   xlsx_wide_series(relation, sheets=none): the time-series sheets (all sheets when none) of a workbook in long form. A sheet holds one
   or more blocks; a block starts at a header row (a row with at least three date cells from column 2 on)
   and its title is the nearest text in column 1 above the header. Every row under the header (until the
   next header) whose column 1 is text is an item; its numeric cells under a header date are the values.
   One output row per sheet × block × item × date: item_label (column 1, trimmed), item_occurrence (the
   n-th item with that label in the block, so repeated labels such as "EUR" stay apart), item_code
   (column 2 text when the header's column 2 is text, e.g. ESA codes), period_end (the header date),
   value (as stored), value_format (the cell's number format; '%' formats are fractions). Cells that are
   not numbers ("b.d.", "-", "ND") are left out. #}

{% macro xlsx_newest_cells(relation) %}
    select *
    from (
        select *
        from {{ relation }}
        qualify row_number() over (partition by raw_sha256, source_row_number order by ingested_at) = 1
    )
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc, raw_sha256) = 1
{% endmacro %}

{% macro xlsx_wide_series(relation, sheets=none) %}
    with cells as (
        select
            *,
            try_cast(value_text as double) as number_value,
            cell_type = 'n'
            and number_format is not null
            and regexp_matches(lower(number_format), 'y')
            and not contains(number_format, '#') as is_date
        from ({{ xlsx_newest_cells(relation) }})
        {% if sheets %}where sheet_name in ({% for s in sheets %}'{{ s }}'{% if not loop.last %}, {% endif %}{% endfor %}){% endif %}
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
        select entity, sheet_name, row_number as header_row
        from date_cells
        group by all
        having count(*) >= 3
    ),

    headers as (
        select d.entity, d.sheet_name, d.row_number as header_row, d.column_number, d.period_end
        from date_cells as d
        inner join header_rows as h
            on d.entity = h.entity and d.sheet_name = h.sheet_name and d.row_number = h.header_row
    ),

    texts as (
        select entity, sheet_name, row_number, column_number, trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text
        from cells
        where cell_type in ('s', 'inlineStr', 'str') and trim(value_text) <> ''
    ),

    header_code as (
        select h.entity, h.sheet_name, h.header_row
        from header_rows as h
        inner join texts as t
            on h.entity = t.entity and h.sheet_name = t.sheet_name and h.header_row = t.row_number and t.column_number = 2
    ),

    items as (
        select
            t.entity,
            t.sheet_name,
            t.row_number,
            t.text as item_label,
            (
                select max(h.header_row)
                from header_rows as h
                where h.entity = t.entity and h.sheet_name = t.sheet_name and h.header_row < t.row_number
            ) as header_row
        from texts as t
        where t.column_number = 1
    ),

    titled as (
        select
            i.*,
            (
                select arg_max(t.text, t.row_number)
                from texts as t
                where t.entity = i.entity and t.sheet_name = i.sheet_name and t.column_number = 1
                    and t.row_number < i.header_row
                    and t.row_number > coalesce((
                        select max(h.header_row) from header_rows as h
                        where h.entity = i.entity and h.sheet_name = i.sheet_name and h.header_row < i.header_row
                    ), 0)
            ) as block_title,
            row_number() over (
                partition by i.entity, i.sheet_name, i.header_row, i.item_label order by i.row_number
            ) as item_occurrence
        from items as i
        where i.header_row is not null
    )

    select
        it.entity,
        it.sheet_name,
        it.header_row,
        coalesce(it.block_title, it.sheet_name) as block_title,
        it.row_number as source_row,
        it.item_label,
        it.item_occurrence,
        code.text as item_code,
        h.period_end,
        c.number_value as value,
        c.number_format as value_format,
        c.raw_sha256,
        c.source_version,
        c.ingested_at
    from titled as it
    inner join headers as h
        on it.entity = h.entity and it.sheet_name = h.sheet_name and it.header_row = h.header_row
    inner join cells as c
        on c.entity = it.entity and c.sheet_name = it.sheet_name and c.row_number = it.row_number
        and c.column_number = h.column_number
    left join header_code as hc
        on it.entity = hc.entity and it.sheet_name = hc.sheet_name and it.header_row = hc.header_row
    left join texts as code
        on hc.header_row is not null and code.entity = it.entity and code.sheet_name = it.sheet_name
        and code.row_number = it.row_number and code.column_number = 2
    where c.cell_type = 'n' and not c.is_date and c.number_value is not null
{% endmacro %}
