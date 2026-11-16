{# SIO graduates (register PLS-022): the nine graduates workbooks of cie.gov.pl (entities absolwenci_*, data as of
   30 September 2010 to 2018: graduates of the school year that ended that year). Two layouts, both found by text:
   - 2010-2017: one sheet per pupil category ("Kategoria uczniów (001) Dzieci lub młodzież", 002 adults, 003
     without category); the header row holds 'Liczba absolwentów' (total) and 'w tym dziewcząt' (of which girls)
     per school type; the type is the text one row above ('(00003) Szkoła podstawowa'), or the name one row and the
     type id two rows above (2017); 'Razem' columns are the published totals per category (school_type_code '_T').
     Rows: a voivodeship id in column 1 with 'WOJ. <NAME>' beside it, or the Poland row ('POLSKA', 'Razem').
   - 2018: a tidy table (header row with 'Województwo Id'): voivodeship, school type (name only), pupil category,
     'Absolwenci' and 'Absolwenci - dziewczęta'; 'Razem' rows are Poland.
   Year: from the entity name (30092010, ix2014, ...). Grain: period_year x area_code x pupil_category_code x
   school_type_name x sex_code. Values as published; an empty cell is no row. #}
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
    where entity like 'absolwenci%'
),

tidy_header as (
    select
        entity,
        sheet_index,
        min(r) as header_row
    from cells
    where v = 'Województwo Id'
    group by all
),

-- 2018: tidy table
tidy_body as (
    select
        d.entity,
        d.sheet_index,
        d.r,
        h.v as label,
        d.v,
        d.raw_sha256,
        d.source_version
    from tidy_header as t
    inner join cells as h on t.entity = h.entity and t.sheet_index = h.sheet_index and t.header_row = h.r
    inner join cells as d
        on h.entity = d.entity and h.sheet_index = d.sheet_index and h.c = d.c and t.header_row < d.r
),

tidy_keys as (
    select
        entity,
        sheet_index,
        r,
        max(v) filter (where label = 'Województwo Id') as area_id,
        max(v) filter (where label = 'Województwo') as area_name,
        max(v) filter (where label = 'Typ jednostki') as school_type_name,
        max(v) filter (where label = 'Kategoria uczniów') as pupil_category
    from tidy_body
    group by all
),

tidy as (
    select
        b.entity,
        case when k.area_id in ('Razem', 'RAZEM') then 'PL' else lpad(k.area_id, 2, '0') end as area_code,
        coalesce(k.area_name, k.area_id) as area_name,
        k.pupil_category,
        case
            when k.school_type_name is null or k.school_type_name in ('Razem', 'RAZEM') then '_T'
        end as school_type_code,
        coalesce(k.school_type_name, 'Razem') as school_type_name,
        case b.label
            when 'Absolwenci' then 'T'
            when 'Absolwenci - dziewczęta' then 'F'
            else 'unknown:' || b.label
        end as sex_code,
        b.label as measure_label,
        b.v as value_text,
        b.raw_sha256,
        b.source_version
    from tidy_body as b
    inner join tidy_keys as k on b.entity = k.entity and b.sheet_index = k.sheet_index and b.r = k.r
    where b.label not in ('Województwo Id', 'Województwo', 'Typ jednostki', 'Kategoria uczniów')
),

-- 2010-2017: one cross-tab per category sheet
cross_header as (
    select
        c.entity,
        c.sheet_index,
        min(c.r) as header_row
    from cells as c
    anti join tidy_header as t on c.entity = t.entity
    where lower(c.v) = 'liczba absolwentów'
    group by all
),

cross_category as (
    select
        c.entity,
        c.sheet_index,
        min(c.v) as pupil_category
    from cells as c
    inner join cross_header as h on c.entity = h.entity and c.sheet_index = h.sheet_index
    where c.v like 'Kategoria uczniów%' and c.r < h.header_row
    group by all
),

cross_columns as (
    select
        c.entity,
        c.sheet_index,
        h.header_row,
        c.c,
        c.v as measure_label,
        case
            when lower(c.v) = 'liczba absolwentów' then 'T'
            when lower(c.v) like 'w tym dziewcz%' then 'F'
        end as sex_code
    from cells as c
    inner join cross_header as h on c.entity = h.entity and c.sheet_index = h.sheet_index and c.r = h.header_row
    where lower(c.v) = 'liczba absolwentów' or lower(c.v) like 'w tym dziewcz%'
),

cross_types as (
    -- the school type above each total column; its 'of which girls' column is the next one
    select
        k.entity,
        k.sheet_index,
        k.c,
        max(x.v) filter (where x.r = k.header_row - 1) as name_cell,
        max(x.v) filter (where x.r = k.header_row - 2 and regexp_matches(x.v, '^(\d{1,5}|Razem|RAZEM)$')) as code_cell
    from cross_columns as k
    left join cells as x
        on
            k.entity = x.entity and k.sheet_index = x.sheet_index and k.c = x.c
            and x.r in (k.header_row - 1, k.header_row - 2)
    where k.sex_code = 'T'
    group by all
),

cross_rows as (
    select
        c1.entity,
        c1.sheet_index,
        c1.r,
        case when c1.v in ('POLSKA', 'Razem', 'RAZEM') then 'PL' else lpad(c1.v, 2, '0') end as area_code,
        coalesce(c2.v, c1.v) as area_name
    from cells as c1
    inner join cross_header as h on c1.entity = h.entity and c1.sheet_index = h.sheet_index
    left join cells as c2 on c1.entity = c2.entity and c1.sheet_index = c2.sheet_index and c1.r = c2.r and c2.c = 2
    where
        c1.c = 1
        and c1.r > h.header_row
        and (c1.v in ('POLSKA', 'Razem', 'RAZEM') or (regexp_matches(c1.v, '^\d{1,2}$') and c2.v like 'WOJ.%'))
),

crossed as (
    select
        k.entity,
        w.area_code,
        w.area_name,
        g.pupil_category,
        case
            when coalesce(t.name_cell, t.code_cell) in ('Razem', 'RAZEM') then '_T'
            when regexp_matches(t.name_cell, '^\((\d{5})\)') then regexp_extract(t.name_cell, '^\((\d{5})\)', 1)
            when regexp_matches(t.code_cell, '^\d{1,5}$') then lpad(t.code_cell, 5, '0')
        end as school_type_code,
        trim(regexp_replace(coalesce(t.name_cell, t.code_cell), '^\(\d{5}\)', '')) as school_type_name,
        k.sex_code,
        k.measure_label,
        v.v as value_text,
        v.raw_sha256,
        v.source_version
    from cross_columns as k
    inner join cross_types as t
        on
            k.entity = t.entity and k.sheet_index = t.sheet_index
            and t.c = case k.sex_code when 'T' then k.c else k.c - 1 end
    inner join cross_category as g on k.entity = g.entity and k.sheet_index = g.sheet_index
    inner join cross_rows as w on k.entity = w.entity and k.sheet_index = w.sheet_index
    inner join cells as v on w.entity = v.entity and w.sheet_index = v.sheet_index and w.r = v.r and k.c = v.c
),

unioned as (
    select * from tidy
    union all by name
    select * from crossed
)

select
    entity,
    cast(regexp_extract(entity, '(?:3009|ix_?)(20\d{2})', 1) as integer) as period_year,
    area_code,
    area_name,
    {{ poc_cie_sio_category('pupil_category') }} as pupil_category_code,
    pupil_category,
    school_type_code,
    case when school_type_code = '_T' then 'Razem' else school_type_name end as school_type_name,
    sex_code,
    measure_label,
    'persons' as unit_code,
    value_text,
    try_cast(value_text as double) as value,
    source_version,
    raw_sha256
from unioned
