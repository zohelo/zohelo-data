{# GUS publication tables (bronze.<source>_cells of ingestion/common/gus_publications.py and the census workbooks):
   the values of curated tables, found by their title and column headers, never by fixed cell positions, so a
   table that moved to another sheet, workbook or column in a later edition is still found, and a table whose title
   or header no longer matches yields no rows (the Silver tests and the trial counts show it).

   gus_table_values(relation, specs): one row per spec x data row x value cell. A spec is a dict:
     table    short code of the table (kept in Silver)
     title    regex (case-insensitive) on the sheet title = the text of rows 1-3 and of any row up to 8 with a cell
              starting 'Tabl' (sheets that open with a chapter heading)
     measure  measure code; unit: the unit code as published
     header   regex (case-insensitive) on the column's header path: the header rows' texts above the column, each
              carried to the right over its merged area (until a header cell in its own or a higher row starts),
              joined ' | '; title text in columns right of the label column can lead the path, so anchor a
              regex on a path segment, '(^|\| )Ogółem', not on the start
     block    optional regex on the block label (the nearest pure-text row above the data row: "NSP 2021",
              "w liczbach bezwzględnych"); without it every data row of the sheet counts
     period   optional reference year; default the edition year (the entity's last four digits)
     groups   optional [[group_code, label regex], ...]: data rows are the rows whose label matches (survey tables
              of Poland by sex); without it data rows are the voivodeship rows and the Poland row
   Areas: the 16 voivodeships by their names (lower case; area_name) and Poland ('polska': labels "Polska",
   "POLAND", "Polska 2024", "P O L S K A 2018"); a Poland row that names another year than the period (the
   previous year printed above the current one) is left out, and so is a "POLSKA" row without a year when a "POLAND" row
   without a year follows it (older editions print the previous year in Polish, the current one in English).
   The label column is the leftmost column holding a voivodeship name or a group label.
   Values: numbers as stored; GUS symbols: '–' / '-' magnitude zero (value 0, status ok), '.' and '#' no data or
   confidential (no value, status confidential), 'x' not applicable (no value), a number followed by 'v' (low
   precision of a survey estimate) as the number with status low_reliability; any other text is status unparsed
   with no value (a Silver accepted_values test fails on it). #}

{% macro gus_regions() %}
    select * from (
        values
        ('dolnośląskie'), ('kujawsko-pomorskie'), ('lubelskie'), ('lubuskie'), ('łódzkie'), ('małopolskie'),
        ('mazowieckie'), ('opolskie'), ('podkarpackie'), ('podlaskie'), ('pomorskie'), ('śląskie'),
        ('świętokrzyskie'), ('warmińsko-mazurskie'), ('wielkopolskie'), ('zachodniopomorskie')
    ) as t (area_name)
{% endmacro %}

{% macro gus_table_values(relation, specs) %}
    with specs (table_code, title_regex, measure_code, header_regex, unit_code, block_regex, period_year) as (
        values
        {%- for s in specs %}
        (
            '{{ s.table }}', '{{ s.title }}', '{{ s.measure }}', '{{ s.header }}', '{{ s.unit }}',
            {{ "'" ~ s.block ~ "'" if s.block else 'cast(null as varchar)' }},
            {{ s.period if s.period else 'cast(null as integer)' }}
        ){{ "," if not loop.last }}
        {%- endfor %}
    ),

    spec_groups (table_code, measure_code, group_code, group_regex) as (
        select * from (
            values
            ('', '', '', '')
            {%- for s in specs %}{% for g in (s.groups or []) %},
            ('{{ s.table }}', '{{ s.measure }}', '{{ g[0] }}', '{{ g[1] }}')
            {%- endfor %}{% endfor %}
        ) as t (table_code, measure_code, group_code, group_regex)
        where table_code <> ''
    ),

    cells as (
        select
            entity,
            member,
            sheet_name,
            row_number,
            column_number,
            value_text,
            source_version,
            raw_sha256,
            try_cast(regexp_extract(entity, '_([0-9]{4})$', 1) as integer) as edition_year,
            trim(regexp_replace(
                replace(replace(replace(value_text, chr(160), ' '), '–', '-'), '—', '-'), '\s+', ' ', 'g'
            )) as txt
        from ({{ xlsx_newest_cells(relation) }})
    ),

    flagged as (
        select
            *,
            regexp_matches(txt, '^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$') as is_number,
            lower(regexp_replace(txt, '^(?i)województwo\s+', '')) as label_l
        from cells
    ),

    table_rows as (
        select distinct entity, member, sheet_name, row_number
        from flagged
        where row_number <= 8 and regexp_matches(txt, '^(?i)tabl')
    ),

    titles as (
        select f.entity, f.member, f.sheet_name, string_agg(f.txt, ' ' order by f.row_number, f.column_number) as title
        from flagged as f
        left join table_rows as t using (entity, member, sheet_name, row_number)
        where f.row_number <= 3 or t.row_number is not null
        group by all
    ),

    matched as (
        select t.entity, t.member, t.sheet_name, s.*
        from titles as t
        inner join specs as s on regexp_matches(t.title, '(?i)' || s.title_regex)
    ),

    sheets as (
        select distinct entity, member, sheet_name from matched
    ),

    sheet_cells as (
        select c.*
        from flagged as c
        inner join sheets as s using (entity, member, sheet_name)
    ),

    region_labels as (
        select
            c.*,
            case
                when regexp_matches(replace(c.label_l, ' ', ''), '^(polska|poland)') then 'polska'
                when r.area_name is not null then r.area_name
            end as area_name,
            case
                when regexp_matches(replace(c.label_l, ' ', ''), '^poland') then 'en'
                when regexp_matches(replace(c.label_l, ' ', ''), '^polska')
                    and not contains(replace(c.label_l, ' ', ''), 'poland') then 'pl'
            end as poland_kind
        from sheet_cells as c
        left join ({{ gus_regions() }}) as r on c.label_l = r.area_name
    ),

    group_labels as (
        select distinct
            c.entity, c.member, c.sheet_name, c.row_number, c.column_number, g.table_code, g.measure_code, g.group_code
        from sheet_cells as c
        inner join matched as m using (entity, member, sheet_name)
        inner join spec_groups as g
            on m.table_code = g.table_code and m.measure_code = g.measure_code
            and regexp_matches(c.txt, g.group_regex)
    ),

    label_columns as (
        select entity, member, sheet_name, min(column_number) as label_col
        from (
            select entity, member, sheet_name, column_number from region_labels where area_name is not null
            union all
            select entity, member, sheet_name, column_number from group_labels
        )
        group by all
    ),

    numeric_rows as (
        select distinct entity, member, sheet_name, row_number from sheet_cells where is_number
    ),

    region_rows_all as (
        select
            l.entity, l.member, l.sheet_name, l.row_number, l.area_name, l.edition_year, l.poland_kind,
            coalesce(
                try_cast(regexp_extract(l.label_l, '((19|20)[0-9]{2})', 1) as integer),
                try_cast(regexp_extract(nxt.txt, '^((19|20)[0-9]{2})$', 1) as integer)
            ) as label_year
        from region_labels as l
        inner join label_columns as lc using (entity, member, sheet_name)
        inner join numeric_rows as n using (entity, member, sheet_name, row_number)
        left join sheet_cells as nxt
            on l.entity = nxt.entity and l.member = nxt.member and l.sheet_name = nxt.sheet_name
            and l.row_number = nxt.row_number and nxt.column_number = lc.label_col + 1
        where l.area_name is not null and l.column_number = lc.label_col
    ),

    -- two Poland rows without a year ("POLSKA" above "POLAND", older editions): the first is the previous year
    region_rows as (
        select r.*
        from region_rows_all as r
        where not (
            r.poland_kind = 'pl' and r.label_year is null and exists (
                select 1 from region_rows_all as e
                where e.entity = r.entity and e.member = r.member and e.sheet_name = r.sheet_name
                    and e.poland_kind = 'en' and e.label_year is null
            )
        )
    ),

    first_rows as (
        select entity, member, sheet_name, min(row_number) as first_row
        from (
            select entity, member, sheet_name, row_number from region_rows_all
            union all
            select entity, member, sheet_name, row_number from group_labels
        )
        group by all
    ),

    header_cells as (
        select c.entity, c.member, c.sheet_name, c.row_number, c.column_number, c.txt
        from sheet_cells as c
        inner join first_rows as f using (entity, member, sheet_name)
        inner join label_columns as lc using (entity, member, sheet_name)
        where c.row_number < f.first_row and c.column_number > lc.label_col and c.txt <> ''
    ),

    header_rows as (
        select distinct entity, member, sheet_name, row_number from header_cells
    ),

    value_columns as (
        select distinct c.entity, c.member, c.sheet_name, c.column_number
        from sheet_cells as c
        inner join label_columns as lc using (entity, member, sheet_name)
        where c.column_number > lc.label_col
    ),

    header_grid as (
        select v.entity, v.member, v.sheet_name, v.column_number, h.row_number
        from value_columns as v
        inner join header_rows as h using (entity, member, sheet_name)
    ),

    header_spans as (
        select g.entity, g.member, g.sheet_name, g.column_number, g.row_number, hc.column_number as start_col, hc.txt
        from header_grid as g
        asof join header_cells as hc
            on g.entity = hc.entity and g.member = hc.member and g.sheet_name = hc.sheet_name
            and g.row_number = hc.row_number and g.column_number >= hc.column_number
    ),

    header_paths as (
        -- a header cell covers the columns to its right until a header cell in its own or a higher row starts
        select
            s.entity, s.member, s.sheet_name, s.column_number,
            string_agg(s.txt, ' | ' order by s.row_number) as header_path
        from header_spans as s
        where not exists (
            select 1 from header_cells as up
            where up.entity = s.entity and up.member = s.member and up.sheet_name = s.sheet_name
                and up.row_number < s.row_number and up.column_number > s.start_col
                and up.column_number <= s.column_number
        )
        group by all
    ),

    markers as (
        select
            c.entity, c.member, c.sheet_name, c.row_number,
            string_agg(c.txt, ' ' order by c.column_number) as marker
        from sheet_cells as c
        left join numeric_rows as n using (entity, member, sheet_name, row_number)
        where n.row_number is null and c.row_number > 2 and c.txt <> ''
        group by all
    ),

    data_rows as (
        select
            r.entity, r.member, r.sheet_name, r.row_number, r.edition_year, r.label_year,
            r.area_name, cast(null as varchar) as group_code, cast(null as varchar) as g_table,
            cast(null as varchar) as g_measure
        from region_rows as r
        union all
        select
            g.entity, g.member, g.sheet_name, g.row_number,
            try_cast(regexp_extract(g.entity, '_([0-9]{4})$', 1) as integer) as edition_year,
            cast(null as integer) as label_year,
            'polska' as area_name, g.group_code, g.table_code as g_table, g.measure_code as g_measure
        from group_labels as g
        inner join label_columns as lc using (entity, member, sheet_name)
        inner join numeric_rows as n using (entity, member, sheet_name, row_number)
        where g.column_number = lc.label_col
    ),

    blocked as (
        select
            d.*,
            m.marker as block_label
        from data_rows as d
        asof left join markers as m
            on d.entity = m.entity and d.member = m.member and d.sheet_name = m.sheet_name
            and d.row_number > m.row_number
    ),

    picked as (
        select
            m.table_code, m.measure_code, m.unit_code, b.entity, b.member, b.sheet_name, b.row_number,
            hp.column_number, b.area_name, b.group_code, b.block_label, hp.header_path,
            coalesce(m.period_year, b.edition_year) as period_year, b.label_year
        from blocked as b
        inner join matched as m using (entity, member, sheet_name)
        inner join header_paths as hp using (entity, member, sheet_name)
        where
            regexp_matches(hp.header_path, '(?i)' || m.header_regex)
            and (m.block_regex is null or regexp_matches(coalesce(b.block_label, ''), '(?i)' || m.block_regex))
            and (b.g_table is null or (b.g_table = m.table_code and b.g_measure = m.measure_code))
            and (b.g_table is not null) = exists (
                select 1 from spec_groups as sg where sg.table_code = m.table_code and sg.measure_code = m.measure_code
            )
    )

    select
        p.table_code,
        p.measure_code,
        p.unit_code,
        p.area_name,
        coalesce(p.group_code, '_T') as group_code,
        p.period_year,
        p.entity,
        p.member,
        p.sheet_name,
        p.row_number,
        p.column_number,
        p.header_path,
        p.block_label,
        c.value_text,
        case
            when c.is_number then cast(c.txt as double)
            when c.txt in ('-') then 0.0
            when regexp_matches(c.txt, '^-?[0-9]+([.,][0-9]+)? ?v$')
                then cast(replace(regexp_extract(c.txt, '^(-?[0-9]+([.,][0-9]+)?)', 1), ',', '.') as double)
        end as value,
        case
            when c.is_number or c.txt in ('-') then 'ok'
            when regexp_matches(c.txt, '^-?[0-9]+([.,][0-9]+)? ?v$') then 'low_reliability'
            when c.txt in ('.', '#') then 'confidential'
            when lower(c.txt) = 'x' then 'not_applicable'
            else 'unparsed'
        end as cell_status,
        c.source_version,
        c.raw_sha256
    from picked as p
    inner join sheet_cells as c
        on p.entity = c.entity and p.member = c.member and p.sheet_name = c.sheet_name
        and p.row_number = c.row_number and p.column_number = c.column_number
    where p.label_year is null or p.label_year = p.period_year
{% endmacro %}
