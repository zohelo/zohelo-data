{# GDDKiA Generalny Pomiar Ruchu (register PL-ENV-022): table 2, average annual daily traffic (SDRR, vehicles per day)
   per measured road section and vehicle class, national roads (dk) and voivodeship roads (dw), census editions
   2020/21 and 2025, from the newest copy of each landed workbook (bronze.gddkia_gpr_cells). Grain: edition_code x
   road_class x section_code x vehicle_class.

   Read by guards, not by position: a file's header starts at the row whose column 1 begins "Numer odcinka" or
   "Numer punktu" and ends above the column-number row (column 1 = '1', column 2 = '2'); each column's role comes from
   its header text (SDRR total, Motocykle, Sam. osob. [i mikrobusy], Mikrobusy, Lekkie sam. ciężarowe, Sam. ciężarowe
   bez / z przyczepą, Autobusy, Ciągniki rolnicze, Numer drogi, Długość, Nazwa, Nr woj., Uwagi); data rows are the rows
   below whose section number is digits. A missing or repeated role fails the tests (completeness, unique).
   2020/21 counts passenger cars and minibuses together (vehicle_class cars_minibuses), 2025 apart (cars, minibuses).
   The 2020/21 national-roads erratum (21 sections) replaces those sections' values of the main file (from_erratum).
   Values as published; '-' (no measurement on the section) is value_status not_measured with no value, never 0. #}
with cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('gddkia_gpr', 'gddkia_gpr_cells')) }})
),

files as (
    select
        entity,
        regexp_extract(entity, '^gpr(\d{4})_', 1) as edition_code,
        regexp_extract(entity, '^gpr\d{4}_(dk|dw)_', 1) as road_class,
        entity like '%\_errata' escape '\' as from_erratum,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from cells
    group by entity
),

head_start as (
    select
        entity,
        sheet_name,
        min(row_number) as first_row
    from cells
    where column_number = 1 and regexp_matches(text, '^Numer (odcinka|punktu)')
    group by all
),

numbering as (
    select
        a.entity,
        a.sheet_name,
        min(a.row_number) as number_row
    from cells as a
    inner join cells as b
        on a.entity = b.entity and a.sheet_name = b.sheet_name and a.row_number = b.row_number and b.column_number = 2
    inner join head_start as h on a.entity = h.entity and a.sheet_name = h.sheet_name
    where a.column_number = 1 and a.text = '1' and b.text = '2' and a.row_number > h.first_row
    group by all
),

headers as (
    select
        c.entity,
        c.sheet_name,
        c.column_number,
        string_agg(c.text, ' | ' order by c.row_number) as header
    from cells as c
    inner join head_start as h on c.entity = h.entity and c.sheet_name = h.sheet_name
    inner join numbering as n on c.entity = n.entity and c.sheet_name = n.sheet_name
    where c.row_number >= h.first_row and c.row_number < n.number_row and c.text <> ''
    group by all
),

roles as (
    select
        entity,
        sheet_name,
        column_number,
        header,
        case
            when regexp_matches(header, '^Numer (odcinka|punktu)') then 'section_number'
            when regexp_matches(header, '^Numer drogi') then 'road_number'
            when regexp_matches(header, '(^|\| )Długość') then 'length_km'
            when regexp_matches(header, '(^|\| )Nazwa') then 'section_name'
            when regexp_matches(header, '(^|\| )Nr woj') then 'voivodeship_code'
            when regexp_matches(header, '(^|\| )(UWAGI|Uwagi)') then 'remark'
            when regexp_matches(header, '^SDRR') then 'motor_vehicles'
            when regexp_matches(header, '(^|\| )Motocykle') then 'motorcycles'
            when regexp_matches(header, '(^|\| )Sam\. osob.*mikrobus') then 'cars_minibuses'
            when regexp_matches(header, '(^|\| )Sam\. osob') then 'cars'
            when regexp_matches(header, '(^|\| )Mikrobusy') then 'minibuses'
            when regexp_matches(header, '(^|\| )Lekkie') then 'light_goods_vehicles'
            when regexp_matches(header, '(^|\| )bez przycz') then 'trucks_without_trailer'
            when regexp_matches(header, '(^|\| )z przycz') then 'trucks_with_trailer'
            when regexp_matches(header, '(^|\| )Autobusy') then 'buses'
            when regexp_matches(header, '(^|\| )Ciągniki') then 'agricultural_tractors'
        end as role
    from headers
),

-- a file holds one table (one header) and each role in one column; anything else stops the build
checked_roles as (
    select
        r.entity,
        r.sheet_name,
        r.column_number,
        case
            when count(*) over (partition by r.entity, r.role) > 1
                then error('gddkia_gpr_sdrr: ' || r.entity || ' has several columns for ' || r.role)
            when count(distinct r.sheet_name) over (partition by r.entity) > 1
                then error('gddkia_gpr_sdrr: ' || r.entity || ' has a table header on several sheets')
            else r.role
        end as role
    from roles as r
    where r.role is not null
),

data_rows as (
    select
        c.entity,
        c.sheet_name,
        c.row_number
    from cells as c
    inner join checked_roles as r
        on c.entity = r.entity and c.sheet_name = r.sheet_name and c.column_number = r.column_number
    inner join numbering as n on c.entity = n.entity and c.sheet_name = n.sheet_name
    where r.role = 'section_number' and c.row_number > n.number_row and regexp_matches(c.text, '^[0-9]+$')
),

row_cells as (
    select
        d.entity,
        d.sheet_name,
        d.row_number,
        r.role,
        c.text
    from data_rows as d
    inner join cells as c on d.entity = c.entity and d.sheet_name = c.sheet_name and d.row_number = c.row_number
    inner join checked_roles as r
        on c.entity = r.entity and c.sheet_name = r.sheet_name and c.column_number = r.column_number
),

sections as (
    select
        entity,
        sheet_name,
        row_number,
        max(text) filter (where role = 'section_number') as section_number,
        max(text) filter (where role = 'road_number') as road_number,
        max(text) filter (where role = 'section_name') as section_name,
        max(text) filter (where role = 'length_km') as length_text,
        max(text) filter (where role = 'voivodeship_code') as voivodeship_text,
        max(text) filter (where role = 'remark') as remark
    from row_cells
    group by all
),

observations as (
    select
        f.edition_code,
        case f.edition_code when '2020' then '2020/21' else f.edition_code end as edition_label,
        cast(f.edition_code as integer) as edition_year,
        f.road_class,
        lpad(s.section_number, 5, '0') as section_code,
        s.section_number,
        s.road_number,
        s.section_name,
        try_cast(s.length_text as double) as length_km,
        lpad(s.voivodeship_text, 2, '0') as voivodeship_code,
        s.remark,
        f.from_erratum,
        v.role as vehicle_class,
        v.text as value_text,
        case when regexp_matches(v.text, '^[0-9]+(\.[0-9]+)?$') then cast(v.text as double) end as value,
        case
            when regexp_matches(v.text, '^[0-9]+(\.[0-9]+)?$') then 'ok'
            when v.text = '-' then 'not_measured'
            else 'unparsed'
        end as value_status,
        s.entity,
        f.source_version,
        f.raw_sha256
    from sections as s
    inner join files as f on s.entity = f.entity
    inner join row_cells as v
        on s.entity = v.entity and s.sheet_name = v.sheet_name and s.row_number = v.row_number
    where v.role in (
        'motor_vehicles', 'motorcycles', 'cars', 'cars_minibuses', 'minibuses', 'light_goods_vehicles',
        'trucks_without_trailer', 'trucks_with_trailer', 'buses', 'agricultural_tractors'
    )
)

-- the erratum's rows replace the main file's rows of the same section (a later publication of the same values)
select o.*
from observations as o
where
    o.from_erratum
    or not exists (
        select 1
        from observations as e
        where
            e.from_erratum
            and e.edition_code = o.edition_code
            and e.road_class = o.road_class
            and e.section_code = o.section_code
    )
