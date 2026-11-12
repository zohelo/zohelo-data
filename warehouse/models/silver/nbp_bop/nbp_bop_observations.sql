{# NBP balance of payments (monthly bop_m, quarterly bop_q) and international investment position (quarterly
<<<<<<< HEAD
   iip_q): one row per workbook × currency × item × period of the newest Landing copy of each workbook. The
   period comes from the year header and the period label (roman month I-XII; "I kw / I Q" … "IV kw / IV Q");
   a label of neither form is not a period and is left out. item_en is the English half of the bilingual
   label ("Rachunek bieżący / Current account"; one IIP label separates the halves by spaces), else the label. item_code is the item's slug, prefixed
   by its block's and then its section's slug where the slug alone repeats within the workbook and currency
   (equity under direct investment assets and liabilities). Amounts in million units of the currency. #}
with source_rows as (
    select *
    from {{ source('nbp_bop', 'nbp_bop_cells') }}
    qualify row_number() over (
        partition by raw_sha256, sheet, source_row_number, column_number order by ingested_at
    ) = 1
),

newest as (
    select *
    from source_rows
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

labelled as (
    select
        *,
        trim(regexp_replace(sheet, '^IIP ', '')) as currency,
        case entity when 'bop_m' then 'M' else 'Q' end as frequency,
        -- the English half: after " / ", else after a run of spaces ("…długoterminowe  Bonds and notes")
        trim(coalesce(
            nullif(regexp_extract(item_label, '/\s*([^/]+)$', 1), ''),
            nullif(regexp_extract(item_label, '\S\s{2,}([A-Z][^ ].*)$', 1), ''),
            item_label
        )) as item_en,
        trim(coalesce(nullif(regexp_extract(block_label, '/\s*([^/]+)$', 1), ''), block_label)) as block_en,
        trim(coalesce(nullif(regexp_extract(section_label, '/\s*([^/]+)$', 1), ''), section_label)) as section_en,
        trim(regexp_replace(period_text, '\s+', ' ', 'g')) as period_clean
    from newest
),

periods as (
    select
        *,
        try_cast(year_text as integer) as year,
        case
            when frequency = 'M'
                then list_position(
                    ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII'], upper(period_clean)
                )
            else
                (list_position(['I', 'II', 'III', 'IV'], upper(regexp_extract(period_clean, '^([IV]+) kw', 1))) - 1)
                * 3 + 1
        end as start_month,
        regexp_replace(lower(trim(item_en)), '[^a-z0-9]+', '_', 'g') as item_slug,
        regexp_replace(lower(trim(block_en)), '[^a-z0-9]+', '_', 'g') as block_slug,
        regexp_replace(lower(trim(section_en)), '[^a-z0-9]+', '_', 'g') as section_slug
    from labelled
),

placed as (
    select
        *,
        make_date(year, start_month, 1) as date_key,
        count(distinct source_row_number) over (partition by entity, sheet, item_slug) as same_item,
        count(distinct source_row_number) over (partition by entity, sheet, block_slug, item_slug) as same_in_block
    from periods
    where year is not null and start_month is not null and start_month > 0
)

select
    entity,
    currency,
    frequency,
    date_key,
    cast(year as varchar) || case frequency
        when 'M' then '-' || lpad(cast(start_month as varchar), 2, '0')
        else '-Q' || cast((start_month + 2) // 3 as varchar)
    end as period_label,
    trim(both '_' from case
        when same_item = 1 then item_slug
        when item_slug = block_slug then section_slug || '.' || item_slug
        when same_in_block = 1 then block_slug || '.' || item_slug
        else section_slug || '.' || block_slug || '.' || item_slug
    end) as item_code,
    item_label,
    item_en,
    block_label,
    section_label,
    source_row_number,
    try_cast(replace(value_text, ',', '.') as double) as value,
    value_text,
    raw_sha256,
    source_version,
    ingested_at
from placed
=======
   iip_q) from bronze.nbp_bop_cells (ingestion/common/xlsx_cells.py), newest Landing copy of each workbook: one
   row per workbook × currency sheet × item × period. Layout of every sheet: a header row of years (each year
   written once over its months or quarters; the first row with at least three year cells) and under it the
   period labels (I … XII; "I kw / I Q" … "IV kw / IV Q"); item labels left of the first year column; blank
   rows separate blocks of items, the first row of a block is its head. An account or total heading standing
   alone ("Rachunek bieżący / Current account", "Ogółem aktywa / Assets") opens a section.
   item_code is fixed by rule, not by uniqueness: the slug of the English label (after " / ", or after a run
   of spaces in the one IIP label without a slash; known NBP typos normalised), prefixed by the block head
   when it differs from the item, and for the IIP by its section (assets, liabilities); tests/nbp_bop_items.sql
   fails on a code outside the expected list, so a renamed NBP label never becomes a new key silently.
   A value cell outside the period grid, or one that is not a number, is kept with date_key or value null and
   fails tests/nbp_bop_unplaced.sql. Amounts in million units of the currency. #}
with cells as (
    select
        *,
        trim(value_text) as txt
    from ({{ xlsx_newest_cells(source('nbp_bop', 'nbp_bop_cells')) }})
),

year_cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        cast(try_cast(txt as double) as integer) as year
    from cells
    where regexp_matches(txt, '^(19|20)[0-9]{2}(\.0+)?$')
),

header as (  -- the first row with at least three year cells
    select
        entity,
        sheet_name,
        min(row_number) as year_row
    from (
        select
            entity,
            sheet_name,
            row_number
        from year_cells
        group by entity, sheet_name, row_number
        having count(*) >= 3
    ) as rows_with_years
    group by entity, sheet_name
),

header_years as (
    select
        y.entity,
        y.sheet_name,
        y.column_number,
        y.year
    from year_cells as y
    inner join header as h on y.entity = h.entity and y.sheet_name = h.sheet_name and y.row_number = h.year_row
),

first_column as (
    select
        entity,
        sheet_name,
        min(column_number) as first_column
    from header_years
    group by entity, sheet_name
),

grid as (  -- a period column: a label under the year row; its year is the nearest year cell at or left of it
    select
        p.entity,
        p.sheet_name,
        p.column_number,
        regexp_replace(p.txt, '\s+', ' ', 'g') as period_text,
        y.year
    from cells as p
    inner join header as h on p.entity = h.entity and p.sheet_name = h.sheet_name and p.row_number = h.year_row + 1
    inner join first_column as f on p.entity = f.entity and p.sheet_name = f.sheet_name
    asof inner join header_years as y
        on p.entity = y.entity and p.sheet_name = y.sheet_name and p.column_number >= y.column_number
    where p.column_number >= f.first_column
),

labels as (  -- one item per row under the header: the text left of the first year column
    select
        c.entity,
        c.sheet_name,
        c.row_number,
        string_agg(c.txt, ' ' order by c.column_number) as item_label
    from cells as c
    inner join header as h on c.entity = h.entity and c.sheet_name = h.sheet_name
    inner join first_column as f on c.entity = f.entity and c.sheet_name = f.sheet_name
    where c.row_number > h.year_row + 1 and c.column_number < f.first_column and c.txt <> ''
    group by c.entity, c.sheet_name, c.row_number
),

block_starts as (
    select
        *,
        coalesce(row_number - lag(row_number) over (partition by entity, sheet_name order by row_number) <> 1, true)
            as starts_block
    from labels
),

blocks as (
    select
        *,
        sum(case when starts_block then 1 else 0 end) over (
            partition by entity, sheet_name order by row_number rows unbounded preceding
        ) as block_number
    from block_starts
),

items as (
    select
        *,
        first_value(item_label) over (partition by entity, sheet_name, block_number order by row_number) as block_label,
        count(*) over (partition by entity, sheet_name, block_number) as block_rows
    from blocks
),

sections as (
    select
        *,
        last_value(
            case
                when
                    block_rows = 1
                    and regexp_matches(lower(item_label), '^(rachunek|ogółem|międzynarodowa pozycja)')
                    then item_label
            end ignore nulls
        ) over (partition by entity, sheet_name order by row_number rows unbounded preceding) as section_label
    from items
),

english as (
    select
        *,
        {% for col in ['item', 'block', 'section'] %}
            replace(regexp_replace(lower(trim(coalesce(
                nullif(regexp_extract({{ col }}_label, '/\s*([^/]+)$', 1), ''),
                nullif(regexp_extract({{ col }}_label, '\S\s{2,}([A-Z][^ ].*)$', 1), ''),
                {{ col }}_label
            ))), '[^a-z0-9]+', '_', 'g'), 'finsncial', 'financial') as {{ col }}_slug{% if not loop.last %},{% endif %}
        {% endfor %}
    from sections
),

coded as (
    select
        *,
        trim(both '_' from item_slug) as item_part,
        trim(both '_' from block_slug) as block_part,
        case
            when entity = 'iip_q' and trim(both '_' from section_slug) in ('assets', 'liabilities')
                then trim(both '_' from section_slug)
        end as section_part
    from english
),

values_in_rows as (
    select
        c.entity,
        c.sheet_name,
        c.row_number,
        c.column_number,
        c.value_text,
        try_cast(c.txt as double) as value,
        c.raw_sha256,
        c.source_version,
        c.ingested_at
    from cells as c
    inner join first_column as f on c.entity = f.entity and c.sheet_name = f.sheet_name
    where c.column_number >= f.first_column
)

select
    i.entity,
    trim(regexp_replace(i.sheet_name, '^IIP ', '')) as currency,
    case i.entity when 'bop_m' then 'M' else 'Q' end as frequency,
    case
        when i.entity = 'bop_m'
            then make_date(
                g.year,
                list_position(
                    ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII'], upper(g.period_text)
                ),
                1
            )
        else make_date(
            g.year,
            (list_position(['I', 'II', 'III', 'IV'], upper(regexp_extract(g.period_text, '^([IV]+) kw', 1))) - 1)
            * 3 + 1,
            1
        )
    end as date_key,
    cast(g.year as varchar) || ' ' || g.period_text as period_label,
    concat_ws(
        '.',
        case when i.section_part is not null and i.item_part <> i.section_part then i.section_part end,
        case when i.block_part <> i.item_part and i.block_part <> coalesce(i.section_part, '') then i.block_part end,
        i.item_part
    ) as item_code,
    i.item_label,
    i.block_label,
    i.section_label,
    v.row_number as source_row_number,
    v.column_number as source_column_number,
    v.value,
    v.value_text,
    v.raw_sha256,
    v.source_version,
    v.ingested_at
from values_in_rows as v
inner join coded as i on v.entity = i.entity and v.sheet_name = i.sheet_name and v.row_number = i.row_number
left join grid as g on v.entity = g.entity and v.sheet_name = g.sheet_name and v.column_number = g.column_number
>>>>>>> feat/2754-nbp-bop
