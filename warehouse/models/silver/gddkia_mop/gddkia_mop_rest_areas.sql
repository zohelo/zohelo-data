{# GDDKiA rest areas (register PL-ENV-023): the table "Tabela MOP istniejące", existing rest areas (MOP) and parkings
   on national roads, one row per listed rest area of each landed monthly edition, from the newest copy of each edition
   (bronze.gddkia_mop_cells). Grain: entity (edition mop_<yyyy>_<mm>) x lp (the producer's row number "Lp." of that
   edition; the table has no other id, and Lp. is not kept across editions).

   Read by guards, not by position: the header is the row whose column 1 starts "Lp." (Bronze rejects a file without
   it); each column's role comes from its header text; data rows are the rows below whose Lp. is digits. A repeated
   role or a header on two sheets stops the build; a missing role fails the tests (not_null, completeness).
   Values as published (whitespace collapsed, empty = null). Zarządca (manager): kept as text only for GDDKiA, the
   word "Prywatny" and names with a company form (S.A., sp. z o.o., spółka, sp.k., SE), else null with manager_kind
   'other', so a sole trader's personal name never leaves Bronze. #}
with cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        nullif(trim(regexp_replace(value_text, '\s+', ' ', 'g')), '') as text,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('gddkia_mop', 'gddkia_mop_cells')) }})
),

head as (
    select
        entity,
        sheet_name,
        min(row_number) as header_row
    from cells
    where column_number = 1 and regexp_matches(lower(text), '^lp')
    group by all
),

roles as (
    select
        c.entity,
        c.sheet_name,
        c.column_number,
        case
            when regexp_matches(lower(c.text), '^lp') then 'lp'
            when regexp_matches(lower(c.text), '^oddzia') then 'branch'
            when regexp_matches(lower(c.text), '^wojew') then 'voivodeship'
            when regexp_matches(lower(c.text), '^zarz') then 'manager'
            when regexp_matches(lower(c.text), '^miejscowo') then 'locality'
            when regexp_matches(lower(c.text), '^(dane identyfik|nazwa)') then 'rest_area_name'
            when regexp_matches(lower(c.text), '^obecna') then 'current_category'
            when regexp_matches(lower(c.text), '^(kat.*docel|docelowa)') then 'target_category'
        end as role
    from cells as c
    inner join head as h on c.entity = h.entity and c.sheet_name = h.sheet_name and c.row_number = h.header_row
),

checked_roles as (
    select
        entity,
        sheet_name,
        column_number,
        case
            when count(*) over (partition by entity, role) > 1
                then error('gddkia_mop_rest_areas: ' || entity || ' has several columns for ' || role)
            when count(distinct sheet_name) over (partition by entity) > 1
                then error('gddkia_mop_rest_areas: ' || entity || ' has a table header on several sheets')
            else role
        end as role
    from roles
    where role is not null
),

row_cells as (
    select
        c.entity,
        c.row_number,
        r.role,
        c.text,
        c.source_version,
        c.raw_sha256
    from cells as c
    inner join checked_roles as r
        on c.entity = r.entity and c.sheet_name = r.sheet_name and c.column_number = r.column_number
    inner join head as h on c.entity = h.entity and c.sheet_name = h.sheet_name
    where c.row_number > h.header_row
),

listed as (
    select
        entity,
        row_number,
        max(text) filter (where role = 'lp') as lp_text,
        max(text) filter (where role = 'branch') as branch,
        max(text) filter (where role = 'voivodeship') as voivodeship,
        max(text) filter (where role = 'manager') as manager,
        max(text) filter (where role = 'locality') as locality,
        max(text) filter (where role = 'rest_area_name') as rest_area_name,
        max(text) filter (where role = 'current_category') as current_category,
        max(text) filter (where role = 'target_category') as target_category,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from row_cells
    group by all
),

classified as (
    select
        *,
        case
            when manager is null then null
            when regexp_matches(lower(manager), '^gddkia') then 'gddkia'
            when lower(manager) = 'prywatny' then 'private'
            when regexp_matches(lower(manager), '(s\.\s?a\.|sp\.\s?z\s?o\.?\s?o|spółka|sp\.\s?k\.|\bse\b)')
                then 'company'
            else 'other'
        end as manager_kind
    from listed
    where regexp_matches(lp_text, '^[0-9]+$')
)

select
    entity,
    make_date(
        cast(regexp_extract(entity, '^mop_([0-9]{4})_', 1) as integer),
        cast(regexp_extract(entity, '^mop_[0-9]{4}_([0-9]{2})$', 1) as integer),
        1
    ) as edition_month,
    cast(lp_text as integer) as lp,
    branch,
    voivodeship,
    lower(voivodeship) as voivodeship_name,
    case when manager_kind in ('gddkia', 'private', 'company') then manager end as manager_name,
    manager_kind,
    locality,
    rest_area_name,
    current_category,
    target_category,
    row_number as source_row,
    source_version,
    raw_sha256
from classified
