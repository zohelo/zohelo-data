{# SDDS advance release calendar of Poland (register TP-012 NBP, TP-013 Ministry of Finance), from
   bronze.sdds_calendar_cells: the newest copy of each calendar year's workbook (entity calendar_<year>), one row per
   expected release: table (sector sheet), data category, reference period, release date and the month column it is
   listed under. Grain: entity x category x reference_period x release_date. Cells as the Bronze guard accepted them
   (ingestion/to_bronze/sdds_calendar/): "-" and "dziennie" (daily: interest rates, exchange rates, WIG) are no release
   and no row; a cell can hold two releases. publisher by category (closed list below; GUS rows are kept here and left
   out of Gold, the GUS kalendarium of release_calendars covers them); a category without a publisher stays NULL and
   fails the not_null test, so a new category is mapped by a person, never guessed. check_status flags the slips the
   file shows instead of correcting them: before_period_end (released before its period ended), outside_column (the
   date is not in the month the column names), mixed_period_kind (a quarter in a monthly category or the reverse,
   minority kind), period_repeated (one category and period listed with two dates); ok otherwise. #}
with copies as (
    select
        entity,
        raw_sha256,
        source_version,
        ingested_at
    from {{ source('sdds_calendar', 'sdds_calendar_cells') }}
    group by entity, raw_sha256, source_version, ingested_at
    qualify rank() over (partition by entity order by ingested_at desc) = 1
),

cells as (
    select
        c.entity,
        c.sheet_name,
        c.row_number,
        c.column_number,
        c.cell_type,
        trim(c.value_text) as value_text,
        c.raw_sha256,
        c.source_version,
        c.ingested_at
    from {{ source('sdds_calendar', 'sdds_calendar_cells') }} as c
    inner join copies as k on c.raw_sha256 = k.raw_sha256 and c.entity = k.entity
),

titles as (
    select
        entity,
        try_cast(regexp_extract(value_text, '^SDDS Kalendarz Upowszechniania Danych w (20\d\d) r\.$', 1) as integer)
            as calendar_year
    from cells
    where sheet_name = 'Spis tablic' and row_number = 1 and column_number = 2
),

headers as (
    select
        entity,
        sheet_name,
        row_number as header_row
    from cells
    where column_number = 1 and value_text = 'Kategorie'
),

months as (
    select
        c.entity,
        c.sheet_name,
        c.column_number,
        make_date(
            cast(regexp_extract(c.value_text, '^(\d{1,2}) (20\d\d)$', 2) as integer),
            cast(regexp_extract(c.value_text, '^(\d{1,2}) (20\d\d)$', 1) as integer),
            1
        ) as column_month
    from cells as c
    inner join headers as h on c.entity = h.entity and c.sheet_name = h.sheet_name and c.row_number = h.header_row
    where c.column_number >= 3
),

categories as (
    select
        c.entity,
        c.sheet_name,
        c.row_number,
        c.value_text as category,
        case
            when u.cell_type = 'n' then date '1899-12-30' + cast(cast(u.value_text as double) as integer)
        end as last_update
    from cells as c
    inner join headers as h on c.entity = h.entity and c.sheet_name = h.sheet_name and c.row_number > h.header_row
    inner join cells as u
        on c.entity = u.entity and c.sheet_name = u.sheet_name and c.row_number = u.row_number and u.column_number = 2
    where c.column_number = 1
),

publishers as (
    select *
    from (
        values
        ('Rachunki narodowe', 'gus'),
        ('Indeks produkcji', 'gus'),
        ('Pracujący', 'gus'),
        ('Bezrobotni', 'gus'),
        ('Wynagrodzenia', 'gus'),
        ('Wskaźniki cen towarów i usług konsumpcyjnych', 'gus'),
        ('Wskaźniki cen produkcji sprzedanej przemysłu', 'gus'),
        ('Ludność', 'gus'),
        ('Obroty towarowe', 'gus'),
        ('Operacje szczebla centralnego sektora instytucji rządowych i samorządowych', 'mf'),
        ('Dług krajowy Skarbu Państwa', 'mf'),
        ('Dług zagraniczny Skarbu Państwa', 'mf'),
        ('Państwowy dług publiczny', 'mf'),
        ('Zestawienie wybranych pozycji z bilansu monetarnych instytucji finansowych', 'nbp'),
        ('Wybrane dane z bilansu NBP', 'nbp'),
        ('Stopy procentowe', 'nbp'),
        ('Bilans Płatniczy', 'nbp'),
        ('Rezerwy oficjalne brutto', 'nbp'),
        ('Płynne aktywa i pasywa w walutach obcych', 'nbp'),
        ('Międzynarodowa pozycja inwestycyjna', 'nbp'),
        ('Zadłużenie zagraniczne', 'nbp'),
        ('Kursy walutowe', 'nbp')
    ) as t (category, publisher)
    -- Not mapped (no dated cell in the 2026 workbook): "Operacje sektora instytucji rządowych i samorządowych" and
    -- "Warszawski Indeks Giełdowy - WIG" (daily).
),

dated as (
    select
        k.entity,
        k.sheet_name,
        k.category,
        k.last_update,
        m.column_month,
        unnest(regexp_extract_all(c.value_text, '(\d{2}\.\d{2}\.\d{4})\s*\(([^()]*)(?:\)|$)', 1)) as release_text,
        unnest(regexp_extract_all(c.value_text, '(\d{2}\.\d{2}\.\d{4})\s*\(([^()]*)(?:\)|$)', 2)) as period_text,
        c.raw_sha256,
        c.source_version,
        c.ingested_at
    from cells as c
    inner join categories as k on c.entity = k.entity and c.sheet_name = k.sheet_name and c.row_number = k.row_number
    inner join months as m on c.entity = m.entity and c.sheet_name = m.sheet_name and c.column_number = m.column_number
    where c.value_text not in ('-', 'dziennie')
),

parsed as (
    select
        *,
        cast(strptime(release_text, '%d.%m.%Y') as date) as release_date,
        trim(regexp_replace(period_text, '\s+', ' ', 'g')) as reference_period
    from dated
),

periods as (
    select
        *,
        case
            when regexp_matches(reference_period, '^\d{2} 20\d\d$') then 'M'
            when regexp_matches(reference_period, '^(Q[1-4]|[1-4] kw\.) 20\d\d$') then 'Q'
        end as period_frequency,
        case
            when regexp_matches(reference_period, '^\d{2} 20\d\d$')
                then make_date(
                    cast(right(reference_period, 4) as integer), cast(left(reference_period, 2) as integer), 1
                )
            when regexp_matches(reference_period, '^(Q[1-4]|[1-4] kw\.) 20\d\d$')
                then make_date(
                    cast(right(reference_period, 4) as integer),
                    3 * cast(regexp_extract(reference_period, '^Q?([1-4])', 1) as integer) - 2,
                    1
                )
        end as period_start
    from parsed
),

checked as (
    select
        *,
        case period_frequency
            when 'M' then last_day(period_start)
            when 'Q' then last_day(period_start + interval 2 month)
        end as period_end,
        count(*) over (partition by entity, category, reference_period) as period_rows,
        count(*) over (partition by entity, category, period_frequency) as kind_rows,
        count(*) over (partition by entity, category) as category_rows
    from periods
)

select
    c.entity,
    t.calendar_year,
    c.sheet_name as sector,
    c.category,
    p.publisher,
    c.last_update,
    c.column_month,
    c.release_date,
    c.reference_period,
    c.period_frequency,
    c.period_start,
    c.period_end,
    case
        when c.release_date <= c.period_end then 'before_period_end'
        when date_trunc('month', c.release_date) <> c.column_month then 'outside_column'
        when c.kind_rows * 2 < c.category_rows then 'mixed_period_kind'
        when c.period_rows > 1 then 'period_repeated'
        else 'ok'
    end as check_status,
    c.raw_sha256,
    c.source_version,
    c.ingested_at
from checked as c
left join titles as t on c.entity = t.entity
left join publishers as p on c.category = p.category
