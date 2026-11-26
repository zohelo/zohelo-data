{# GUS "Zdrowie i ochrona zdrowia w <year> r." (register PLS-004, health family; #2451): curated tables of each
   edition 2012-, Poland and the 16 voivodeships, reference year = the edition year, from the newest copy of each
   edition (bronze.gus_health_cells). Grain: table_code x measure_code x area_name x group_code x period_year.
   Tables are found by title and column header (macro gus_table_values, warehouse/macros/gus_tables.sql), never by
   cell position:
   - medical_personnel: "Pracownicy medyczni według województw" (TABL. 6, 7, 9 or 10 by edition; not "... posiadający
     prawo wykonywania zawodu" nor "... według wieku i województw"), 31 December: doctors (column Lekarze | razem),
     nurses (Pielęgniarki, the "razem" column where the table splits by sex)
   - general_hospitals: "Szpitale ogólne według województw" (TABL. 11 or 14), hospitals (Szpitale / Liczba szpitali,
     31 December), beds (Łóżka | ogółem, 31 December), beds per 10 thousand population (Łóżka | na 10 tys. ludności),
     inpatients in the year (Leczeni | ogółem: patients entered in the main hospital register)
   Series break: from the 2021 edition GUS marks doctors and nurses "Dane są nieporównywalne z danymi opublikowanymi
   w poprzednich edycjach publikacji" (the 2021 edition restates 2020 doctors as 126,064 against 92,255 in the 2020
   edition), so reference years up to 2020 carry their own measure codes doctors_pre2021 and nurses_pre2021.
   Label repairs before the macro reads the cells, each guarded by exact text (the cells keep their positions):
   - a voivodeship name printed over two rows of the label column ("Warmińsko-" / "mazurskie", 2012-2015): both
     cells read as the full name when the two texts joined are a voivodeship name; the row with the numbers counts;
   - the current-year Poland row printed as the year alone right under the previous year's Poland row
     ("OGÓŁEM TOTAL ... 2013" / "2014", hospital tables of 2014-2016): read as "POLSKA <year>";
   - "OGÓŁEM TOTAL ... <year>" (hospital tables 2012-2016) is a Poland label (poland_label).
   Values as published (persons, facilities, beds, beds per 10,000 population as stored, unrounded in older
   editions); GUS symbols per the macro, except '-': it stays without a value (cell_status nil), never 0.
   table_title and table_notes keep the sheet's title (rows 1-3) and the text under the table (footnotes, "Uwaga",
   "Źródło", Polish and English) as printed. #}
{% set personnel = 'Pracownicy medyczni według województw' %}
{% set hospitals = 'Szpitale ogólne[^ ]* według województw' %}
{% set specs = [
    {'table': 'medical_personnel', 'title': personnel, 'measure': 'doctors',
     'header': '(^|\\| )Lekarze[a-z]?\\)? Doctors[a-z]?\\)? \\| razem', 'unit': 'persons'},
    {'table': 'medical_personnel', 'title': personnel, 'measure': 'nurses',
     'header': '(^|\\| )Pielę-? ?gniarki[a-z]?\\)? Nurses[a-z]?\\)?( \\| razem|$)', 'unit': 'persons'},
    {'table': 'general_hospitals', 'title': hospitals, 'measure': 'hospitals',
     'header': '(^|\\| )(Szpitale( Hospitals)?|Liczba szpitali[a-z]?\\)? Number of hospitals[a-z]?\\)?)( \\||$)',
     'unit': 'facilities'},
    {'table': 'general_hospitals', 'title': hospitals, 'measure': 'beds',
     'header': '(^|\\| )Łóżka[^|]* \\| ogółem', 'unit': 'beds'},
    {'table': 'general_hospitals', 'title': hospitals, 'measure': 'beds_per_10k',
     'header': '(^|\\| )Łóżka[^|]* \\| na 10 tys', 'unit': 'per_10k_population'},
    {'table': 'general_hospitals', 'title': hospitals, 'measure': 'inpatients',
     'header': '(^|\\| )Leczeni[^|]* \\| ogółem', 'unit': 'persons'},
] %}
with h_cells as (
    select
        *,
        trim(regexp_replace(replace(value_text, chr(160), ' '), '\s+', ' ', 'g')) as h_txt
    from {{ source('gus_health', 'gus_health_cells') }}
),

h_regions as (
    {{ gus_regions() }}
),

-- a voivodeship name split over two rows of one column: both cells get the full name
h_split_names as (
    select
        u.raw_sha256,
        u.member,
        u.sheet_name,
        u.column_number,
        u.row_number as upper_row,
        r.area_name
    from h_cells as u
    inner join h_cells as l
        on
            u.raw_sha256 = l.raw_sha256 and u.member = l.member and u.sheet_name = l.sheet_name
            and u.column_number = l.column_number and l.row_number = u.row_number + 1
    inner join h_regions as r
        on lower(regexp_replace(u.h_txt, '\s*-$', '') || '-' || l.h_txt) = r.area_name
    where regexp_matches(u.h_txt, '-$') and regexp_matches(l.h_txt, '^[a-ząćęłńóśźż]+$')
),

-- the year alone right under a Poland label in the same column: the current year's Poland row
h_poland_years as (
    select
        y.raw_sha256,
        y.member,
        y.sheet_name,
        y.column_number,
        y.row_number
    from h_cells as y
    inner join h_cells as p
        on
            y.raw_sha256 = p.raw_sha256 and y.member = p.member and y.sheet_name = p.sheet_name
            and y.column_number = p.column_number and p.row_number = y.row_number - 1
    where
        regexp_matches(y.h_txt, '^(19|20)[0-9]{2}$')
        and regexp_matches(lower(replace(p.h_txt, ' ', '')), '^(polska|poland|ogółem)')
),

h_fixed as (
    select
        c.* exclude (c.value_text, c.h_txt),
        case
            when s.area_name is not null then s.area_name
            when y.row_number is not null then 'POLSKA ' || c.h_txt
            else c.value_text
        end as value_text
    from h_cells as c
    left join h_split_names as s
        on
            c.raw_sha256 = s.raw_sha256 and c.member = s.member and c.sheet_name = s.sheet_name
            and c.column_number = s.column_number and c.row_number in (s.upper_row, s.upper_row + 1)
    left join h_poland_years as y
        on
            c.raw_sha256 = y.raw_sha256 and c.member = y.member and c.sheet_name = y.sheet_name
            and c.column_number = y.column_number and c.row_number = y.row_number
),

h_values as (
    select * from ({{ gus_table_values('h_fixed', specs, '^(polska|poland|ogółem)') }})
),

h_last_rows as (
    select
        raw_sha256,
        member,
        sheet_name,
        max(row_number) as last_row
    from h_values
    group by all
),

h_sheet_text as (
    select
        c.raw_sha256,
        c.member,
        c.sheet_name,
        string_agg(c.h_txt, ' ' order by c.row_number, c.column_number) filter (
            where c.row_number <= 3
        ) as table_title,
        string_agg(c.h_txt, ' ' order by c.row_number, c.column_number) filter (
            where c.row_number > v.last_row and c.h_txt <> ''
        ) as table_notes
    from h_cells as c
    inner join h_last_rows as v
        on c.raw_sha256 = v.raw_sha256 and c.member = v.member and c.sheet_name = v.sheet_name
    group by all
)

select
    v.table_code,
    case
        when v.measure_code in ('doctors', 'nurses') and v.period_year <= 2020 then v.measure_code || '_pre2021'
        else v.measure_code
    end as measure_code,
    v.unit_code,
    v.area_name,
    v.group_code,
    v.period_year,
    v.entity,
    v.member,
    v.sheet_name,
    v.row_number,
    v.column_number,
    v.header_path,
    v.value_text,
    case when trim(v.value_text) in ('-', '–', '—') then null else v.value end as value,
    case when trim(v.value_text) in ('-', '–', '—') then 'nil' else v.cell_status end as cell_status,
    t.table_title,
    t.table_notes,
    v.source_version,
    v.raw_sha256
from h_values as v
left join h_sheet_text as t
    on v.raw_sha256 = t.raw_sha256 and v.member = t.member and v.sheet_name = t.sheet_name
