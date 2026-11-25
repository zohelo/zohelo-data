{# SIO teachers (register PLS-022): the newest teachers workbook of cie.gov.pl (NauczEtatyOsoby2006-2018v5_Woj2018v5,
   entity nauczetatyosoby2006_2018*), which repeats the series of the five older ones. Two blocks on one sheet: Poland
   by reference date (30 September 2006 to 2018) and the 16 voivodeships at the date in the block's title. Each
   group is four rows (Osoby = persons, Pełnozatrudnieni = full-time, Niepełnozatrudnieni = part-time, Etaty =
   full-time posts) under a header row of professional grades (1.Stażysta ... 4.Dyplomowany, Razem).
   Found by text, never by position: the header row holds 'Razem' and the four numbered grades; a row's measure is
   its cell with one of the four labels; its group (date or voivodeship) is the nearest label in the column to the
   left, at or above the row; the voivodeship block's date is read from the nearest title above its header.
   Grain: reference_date x area_code x measure_code x grade_code. Values as stored (posts are fractional). #}
with cells as (
    select
        entity,
        sheet_index,
        row_number as r,
        column_number as c,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as v,
        raw_sha256,
        source_version
    from ({{ xlsx_newest_cells(source('cie_sio', 'cie_sio_cells')) }})
    where entity like 'nauczetatyosoby2006%'
),

headers as (
    select
        entity,
        sheet_index,
        r as header_row
    from cells
    group by all
    having
        count(*) filter (where v = 'Razem') = 1
        and count(*) filter (where regexp_matches(v, '^[1-4]\.\s*(Stażysta|Kontraktowy|Mianowany|Dyplomowany)$')) = 4
),

grades as (
    select
        h.entity,
        h.sheet_index,
        h.header_row,
        c.c,
        c.v as grade_label,
        case
            when c.v = 'Razem' then '_T'
            else lower(regexp_extract(c.v, '^[1-4]\.\s*(\S+)$', 1))
        end as grade_code
    from headers as h
    inner join cells as c on h.entity = c.entity and h.sheet_index = c.sheet_index and h.header_row = c.r
    where c.v = 'Razem' or regexp_matches(c.v, '^[1-4]\.')
),

titles as (
    select
        entity,
        sheet_index,
        r as title_row,
        v as title,
        coalesce(
            try_strptime(regexp_extract(v, '(\d{2}\.\d{2}\.\d{4})', 1), '%d.%m.%Y'),
            try_strptime(regexp_extract(v, 'stan na (\d{4})', 1) || '-09-30', '%Y-%m-%d')
        )::date as title_date
    from cells
    where v like 'Liczba nauczycieli%'
),

voivodeships as ({{ poc_cie_sio_voivodeships() }}),

groups as (
    select
        c.entity,
        c.sheet_index,
        c.r as group_row,
        c.c as group_col,
        c.v as group_label,
        try_strptime(regexp_extract(c.v, '^(\d{2}\.\d{2}\.\d{4})', 1), '%d.%m.%Y')::date as group_date,
        w.area_code
    from cells as c
    left join voivodeships as w on replace(lower(c.v), '-', '') = w.name_key
    where regexp_matches(c.v, '^\d{2}\.\d{2}\.\d{4}') or w.area_code is not null
),

measure_rows as (
    select
        entity,
        sheet_index,
        r,
        c as label_col,
        v as measure_label,
        case v
            when 'Osoby' then 'persons'
            when 'Pełnozatrudnieni' then 'full_time'
            when 'Niepełnozatrudnieni' then 'part_time'
            when 'Etaty' then 'posts'
        end as measure_code,
        raw_sha256,
        source_version
    from cells
    where v in ('Osoby', 'Pełnozatrudnieni', 'Niepełnozatrudnieni', 'Etaty')
),

placed as (
    select
        m.*,
        h.header_row,
        g.group_label,
        g.group_date,
        g.area_code
    from measure_rows as m
    asof left join headers as h
        on m.entity = h.entity and m.sheet_index = h.sheet_index and m.r > h.header_row
    asof left join groups as g
        on
            m.entity = g.entity and m.sheet_index = g.sheet_index
            and m.label_col - 1 = g.group_col and m.r >= g.group_row
),

dated as (
    select
        p.*,
        t.title,
        t.title_date
    from placed as p
    asof left join titles as t
        on p.entity = t.entity and p.sheet_index = t.sheet_index and p.header_row > t.title_row
)

select
    d.entity,
    coalesce(d.group_date, d.title_date) as reference_date,
    year(coalesce(d.group_date, d.title_date)) as period_year,
    case when d.group_date is not null then 'PL' else d.area_code end as area_code,
    case when d.group_date is not null then 'Polska' else d.group_label end as area_name,
    d.group_label,
    d.measure_code,
    d.measure_label,
    case d.measure_code when 'posts' then 'fte' else 'persons' end as unit_code,
    g.grade_code,
    g.grade_label,
    v.v as value_text,
    try_cast(v.v as double) as value,
    d.source_version,
    d.raw_sha256
from dated as d
inner join grades as g on d.entity = g.entity and d.sheet_index = g.sheet_index and d.header_row = g.header_row
inner join cells as v on d.entity = v.entity and d.sheet_index = v.sheet_index and d.r = v.r and g.c = v.c
