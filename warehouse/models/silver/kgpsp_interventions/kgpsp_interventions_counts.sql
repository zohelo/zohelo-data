{# KG PSP standard summary tables (register EX-003): table 1, incidents by type and size (fires P, local hazards MZ,
   false alarms AF, their size classes and the total), and table 10, casualties of fires and local hazards (counts of
   deaths and injured by group: rescuers, of which firefighters, other persons, of which children; no persons), per
   year 2016 on, from the newest copy of each landed year (bronze.kgpsp_interventions_cells).
   Grain: report_year x table_code x area_code x measure_code.

   Two layouts, both read by header text, never by position:
     2016-2022  one voivodeship workbook per table: the area column is headed 'Podział administracyjny'; area rows are
                'Polska' (area_code PL) and the 16 voivodeships (names as printed, TERYT code from the static list);
     2023 on    one workbook per table with a row per gmina: the area column is headed 'TERYT' (six digits WWPPGG),
                voivodeship and gmina in the columns headed 'Województwo' and 'Gmina'; no Poland or voivodeship rows.
   The header is the rows from the first one naming '(P)' or '(MZ)' down to the row above the first area row; a value
   column's label is its own text in that last row plus, for each row above, the nearest text at or left of it (merged
   group headings such as 'Pożar (P)'). measure_code from the label: table 1 the source's own code (P, P/M, ..., A/IW,
   OGÓŁEM); table 10 <event>:<group>:<outcome> (P, MZ or OGÓŁEM; ratownicy, strazacy, inne_osoby, dzieci or ogolem;
   smiertelne or ranni). A label that yields no code leaves measure_code null and fails not_null.
   Values as published; a cell that is not a number is value_status 'not_numeric' without a value, never 0. #}
with cells as (
    select
        entity,
        member,
        sheet_name,
        row_number,
        column_number,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text,
        cell_type,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('kgpsp_interventions', 'kgpsp_interventions_cells')) }})
),

voivodeships as (
    select *
    from (
        values
        ('02', 'dolnośląskie'), ('04', 'kujawsko-pomorskie'), ('06', 'lubelskie'), ('08', 'lubuskie'),
        ('10', 'łódzkie'), ('12', 'małopolskie'), ('14', 'mazowieckie'), ('16', 'opolskie'),
        ('18', 'podkarpackie'), ('20', 'podlaskie'), ('22', 'pomorskie'), ('24', 'śląskie'),
        ('26', 'świętokrzyskie'), ('28', 'warmińsko-mazurskie'), ('30', 'wielkopolskie'),
        ('32', 'zachodniopomorskie')
    ) as t (voivodeship_code, voivodeship_name)
),

-- the column naming the area, one per workbook (a second one repeats the rows and fails the unique test)
label_columns as (
    select
        entity,
        member,
        sheet_name,
        row_number as label_row,
        column_number as label_col,
        case when text = 'TERYT' then 'gmina' else 'voivodeship' end as layout
    from cells
    where text in ('Podział administracyjny', 'TERYT')
),

named_columns as (
    select
        l.entity,
        l.member,
        l.sheet_name,
        max(c.column_number) filter (where c.text = 'Gmina') as gmina_col,
        max(c.column_number) filter (where c.text = 'Województwo') as voivodeship_col
    from label_columns as l
    inner join cells as c
        on l.entity = c.entity and l.member = c.member and l.sheet_name = c.sheet_name and l.label_row = c.row_number
    group by all
),

area_rows as (
    select
        c.entity,
        c.member,
        c.sheet_name,
        c.row_number,
        l.layout,
        case
            when l.layout = 'gmina' then 'gmina'
            when lower(c.text) = 'polska' then 'country'
            else 'voivodeship'
        end as area_level,
        case
            when l.layout = 'gmina' then lpad(c.text, 6, '0')
            when lower(c.text) = 'polska' then 'PL'
            else v.voivodeship_code
        end as area_code,
        case when l.layout = 'gmina' then g.text else c.text end as area_name,
        case when l.layout = 'gmina' then left(lpad(c.text, 6, '0'), 2) else v.voivodeship_code end
            as voivodeship_code,
        lower(case when l.layout = 'gmina' then w.text else v.voivodeship_name end) as voivodeship_name
    from cells as c
    inner join label_columns as l
        on
            c.entity = l.entity and c.member = l.member and c.sheet_name = l.sheet_name
            and c.column_number = l.label_col and c.row_number > l.label_row
    left join named_columns as n on c.entity = n.entity and c.member = n.member and c.sheet_name = n.sheet_name
    left join cells as g
        on
            c.entity = g.entity and c.member = g.member and c.sheet_name = g.sheet_name
            and c.row_number = g.row_number and n.gmina_col = g.column_number
    left join cells as w
        on
            c.entity = w.entity and c.member = w.member and c.sheet_name = w.sheet_name
            and c.row_number = w.row_number and n.voivodeship_col = w.column_number
    left join voivodeships as v on l.layout = 'voivodeship' and lower(c.text) = v.voivodeship_name
    where
        (l.layout = 'voivodeship' and (lower(c.text) = 'polska' or v.voivodeship_code is not null))
        or (l.layout = 'gmina' and regexp_matches(c.text, '^[0-9]{5,6}$'))
),

first_area as (
    select
        entity,
        member,
        sheet_name,
        min(row_number) as first_row
    from area_rows
    group by all
),

header_cells as (
    select
        c.entity,
        c.member,
        c.sheet_name,
        c.row_number,
        c.column_number,
        c.text,
        c.row_number = f.first_row - 1 as is_leaf
    from cells as c
    inner join first_area as f on c.entity = f.entity and c.member = f.member and c.sheet_name = f.sheet_name
    where
        c.row_number < f.first_row
        and c.text <> ''
        and c.row_number >= (
            select min(s.row_number)
            from cells as s
            where
                s.entity = c.entity and s.member = c.member and s.sheet_name = c.sheet_name
                and s.row_number < f.first_row and regexp_matches(s.text, '\((P|MZ)\)')
        )
),

first_value_col as (
    select
        entity,
        member,
        sheet_name,
        min(column_number) as first_col
    from header_cells
    where regexp_matches(text, '\((P|MZ|AF)\)|\((P|MZ|A)/|^RAZEM|OGÓŁEM|Śmiertelne|Ranni')
    group by all
),

-- a heading in a row above the last header row covers the columns up to the next heading of that row
header_spans as (
    select
        h.entity,
        h.member,
        h.sheet_name,
        h.row_number,
        h.column_number as from_col,
        case
            when h.is_leaf then h.column_number
            else
                coalesce(lead(h.column_number) over (
                    partition by h.entity, h.member, h.sheet_name, h.row_number order by h.column_number
                ) - 1, 100000)
        end as to_col,
        h.text
    from header_cells as h
    inner join first_value_col as f on h.entity = f.entity and h.member = f.member and h.sheet_name = f.sheet_name
    where h.column_number >= f.first_col
),

value_cells as (
    select
        a.*,
        c.column_number,
        c.text as value_text,
        c.source_version,
        c.raw_sha256
    from area_rows as a
    inner join cells as c
        on a.entity = c.entity and a.member = c.member and a.sheet_name = c.sheet_name and a.row_number = c.row_number
    inner join first_value_col as f on a.entity = f.entity and a.member = f.member and a.sheet_name = f.sheet_name
    where c.column_number >= f.first_col and c.text <> ''
),

labels as (
    select
        v.entity,
        v.member,
        v.sheet_name,
        v.column_number,
        regexp_extract(regexp_extract(v.member, '[^/]*$'), '^(1|10)[._ ]', 1) as table_code,
        string_agg(s.text, ' | ' order by s.row_number) as measure_label
    from (
        select distinct
            entity,
            member,
            sheet_name,
            column_number
        from value_cells
    ) as v
    inner join header_spans as s
        on
            v.entity = s.entity and v.member = s.member and v.sheet_name = s.sheet_name
            and v.column_number between s.from_col and s.to_col
    group by all
),

coded as (
    select
        *,
        case
            when measure_label like '%(P)%' then 'P'
            when measure_label like '%(MZ)%' then 'MZ'
            when measure_label like '%(AF)%' then 'AF'
            when measure_label like '%OGÓŁEM%' then 'OGÓŁEM'
        end as event_code,
        case
            when measure_label like '%w tym strażacy%' then 'strazacy'
            when measure_label like '%w tym dzieci%' then 'dzieci'
            when measure_label like '%Ratownicy%' then 'ratownicy'
            when measure_label like '%Inne osoby%' then 'inne_osoby'
            when measure_label like '%OGÓŁEM%' then 'ogolem'
        end as group_code,
        case
            when measure_label like '%Śmiertelne%' then 'smiertelne'
            when measure_label like '%Ranni%' then 'ranni'
        end as outcome_code
    from labels
)

select
    cast(regexp_extract(v.entity, '^tabele_(\d{4})$', 1) as integer) as report_year,
    l.table_code,
    v.layout,
    v.area_level,
    v.area_code,
    v.area_name,
    v.voivodeship_code,
    v.voivodeship_name,
    case l.table_code
        when '1' then coalesce(nullif(regexp_extract(l.measure_label, '\(((P|MZ|A)/[^)]+)\)', 1), ''), l.event_code)
        when '10' then l.event_code || ':' || l.group_code || ':' || l.outcome_code
    end as measure_code,
    l.measure_label,
    v.value_text,
    case when regexp_matches(v.value_text, '^-?[0-9]+(\.[0-9]+)?$') then cast(v.value_text as double) end as value,
    case when regexp_matches(v.value_text, '^-?[0-9]+(\.[0-9]+)?$') then 'ok' else 'not_numeric' end as value_status,
    v.entity,
    v.member,
    v.source_version,
    v.raw_sha256
from value_cells as v
inner join coded as l
    on v.entity = l.entity and v.member = l.member and v.sheet_name = l.sheet_name and v.column_number = l.column_number
