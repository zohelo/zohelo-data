{# Ministry of Justice ISWS multi-year workbooks (register PLG-031), Poland, annual, from the newest copy of each
   landed workbook (bronze.ms_isws_cells). Grain: table_code x measure_code x breakdown_code x period_year.
   Every value is found by its row label and column header, never by a fixed cell position:
     caseload       "Ewidencja spraw w sądach powszechnych w Polsce": cases received (Wpływ), resolved
                    (Załatwienie) and pending at the year end (Pozostałość) in the common courts, years 2011-
                    in a header row with the three measures under each year; rows OGÓŁEM and the six case types
                    (SPRAWY KARNE, CYWILNE, RODZINNE, Z ZAKRESU PRAWA PRACY, Z ZAKRESU UBEZPIECZEŃ SPOŁECZNYCH,
                    GOSPODARCZE; footnote letters dropped); court-level rows are left out
     duration       "Średni czas trwania postępowania sądowego": one sheet per year (2011-; the sheet "I kw. 2022"
                    of the first quarter is left out), column "Średni czas trwania postęp. sądowego (w miesiącach)",
                    rows Ogółem, Sądy okręgowe, Sądy rejonowe
     adoptions      "Adopcje - przysposobienie", sheet titled "Ewidencja spraw o przysposobienie" (not the sheet of
                    dissolved adoptions): one row per year (2000-), columns Wpłynęło, Załatwiono, "w tym orzeczono"
                    (adoptions granted, cases) and Pozostało na następny okres
     notarial_acts  "Akty notarialne": per sheet (1991-1997, 1998-2012, 2013-) the row "Akty notarialne razem"
                    holds the years and the next row, which has no label of its own, the totals; a label in that
                    row means the layout changed and the sheet yields nothing (the completeness test fails)
   Values as published: numbers (text with thousands spaces "14 553 725" read as numbers); '•' (brak danych) is
   status missing with no value, 'x' not applicable with no value, any other text unparsed with no value (the
   accepted_values test on cell_status fails on it). Empty cells are no rows, never zeros. #}
with cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        value_text,
        source_version,
        raw_sha256,
        trim(regexp_replace(replace(value_text, chr(160), ' '), '\s+', ' ', 'g')) as txt
    from ({{ xlsx_newest_cells(source('ms_isws', 'ms_isws_cells')) }})
),

typed as (
    select
        *,
        lower(regexp_replace(txt, '\s+[a-z]\)(\s*,\s*[a-z]\))*$', '')) as label_l,
        regexp_replace(txt, '\s', '', 'g') as compact
    from cells
),

vals as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        value_text,
        source_version,
        raw_sha256,
        case
            when regexp_matches(compact, '^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$') then cast(compact as double)
        end as value,
        case
            when regexp_matches(compact, '^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$') then 'ok'
            when txt = '•' then 'missing'
            when lower(txt) = 'x' then 'not_applicable'
            else 'unparsed'
        end as cell_status
    from typed
),

caseload_years as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        cast(txt as integer) as period_year
    from typed
    where entity = 'caseload' and row_number <= 8 and column_number >= 2 and regexp_matches(txt, '^(19|20)[0-9]{2}$')
),

caseload_measures as (
    select
        m.entity,
        m.sheet_name,
        m.row_number as header_row,
        m.column_number,
        case m.label_l when 'wpływ' then 'incoming' when 'załatwienie' then 'resolved' else 'pending' end
            as measure_code,
        (
            select arg_max(y.period_year, y.column_number)
            from caseload_years as y
            where
                y.entity = m.entity and y.sheet_name = m.sheet_name
                and y.row_number < m.row_number and y.column_number <= m.column_number
        ) as period_year
    from typed as m
    where m.entity = 'caseload' and m.row_number <= 8 and m.label_l in ('wpływ', 'załatwienie', 'pozostałość')
),

caseload_rows as (
    select
        entity,
        sheet_name,
        row_number,
        case label_l
            when 'ogółem' then 'total'
            when 'sprawy karne' then 'criminal'
            when 'sprawy cywilne' then 'civil'
            when 'sprawy rodzinne' then 'family'
            when 'sprawy z zakresu prawa pracy' then 'labour'
            when 'sprawy z zakresu ubezpieczeń społecznych' then 'social_insurance'
            when 'sprawy gospodarcze' then 'commercial'
        end as breakdown_code
    from typed
    where entity = 'caseload' and column_number = 1
),

caseload as (
    select
        'caseload' as table_code,
        m.measure_code,
        r.breakdown_code,
        'cases' as unit_code,
        m.period_year,
        v.*
    from caseload_rows as r
    inner join caseload_measures as m
        on r.entity = m.entity and r.sheet_name = m.sheet_name and r.row_number > m.header_row
    inner join vals as v
        on
            r.entity = v.entity and r.sheet_name = v.sheet_name and r.row_number = v.row_number
            and m.column_number = v.column_number
    where r.breakdown_code is not null
),

duration_columns as (
    select
        entity,
        sheet_name,
        row_number as header_row,
        column_number,
        cast(sheet_name as integer) as period_year
    from typed
    where
        entity = 'duration' and regexp_matches(sheet_name, '^(19|20)[0-9]{2}$')
        and row_number <= 8 and column_number >= 2 and starts_with(label_l, 'średni czas')
),

duration as (
    select
        'duration' as table_code,
        'avg_duration' as measure_code,
        case r.label_l when 'ogółem' then 'total' when 'sądy okręgowe' then 'regional' else 'district' end
            as breakdown_code,
        'months' as unit_code,
        c.period_year,
        v.*
    from typed as r
    inner join duration_columns as c
        on r.entity = c.entity and r.sheet_name = c.sheet_name and r.row_number > c.header_row
    inner join vals as v
        on
            r.entity = v.entity and r.sheet_name = v.sheet_name and r.row_number = v.row_number
            and c.column_number = v.column_number
    where r.entity = 'duration' and r.column_number = 1 and r.label_l in ('ogółem', 'sądy okręgowe', 'sądy rejonowe')
),

adoption_sheets as (
    select distinct
        entity,
        sheet_name
    from typed
    where entity = 'adoptions' and row_number <= 3 and contains(label_l, 'spraw o przysposobienie')
),

adoption_header as (
    select
        t.entity,
        t.sheet_name,
        t.row_number as header_row
    from typed as t
    inner join adoption_sheets as s on t.entity = s.entity and t.sheet_name = s.sheet_name
    where t.column_number = 1 and t.label_l = 'lata'
),

adoption_measures as (
    select
        t.entity,
        t.sheet_name,
        h.header_row,
        t.column_number,
        case
            when starts_with(t.label_l, 'wpłynęło') then 'incoming'
            when starts_with(t.label_l, 'załatwiono') then 'resolved'
            when starts_with(t.label_l, 'w tym orzeczono') then 'granted'
            when starts_with(t.label_l, 'pozostało') then 'pending'
        end as measure_code
    from typed as t
    inner join adoption_header as h
        on t.entity = h.entity and t.sheet_name = h.sheet_name and t.row_number = h.header_row
    where t.column_number >= 2
),

adoptions as (
    select
        'adoptions' as table_code,
        m.measure_code,
        'total' as breakdown_code,
        'cases' as unit_code,
        cast(r.txt as integer) as period_year,
        v.*
    from typed as r
    inner join adoption_measures as m
        on r.entity = m.entity and r.sheet_name = m.sheet_name and r.row_number > m.header_row
    inner join vals as v
        on
            r.entity = v.entity and r.sheet_name = v.sheet_name and r.row_number = v.row_number
            and m.column_number = v.column_number
    where r.column_number = 1 and regexp_matches(r.txt, '^(19|20)[0-9]{2}$') and m.measure_code is not null
),

notarial_header as (
    select
        entity,
        sheet_name,
        row_number as header_row
    from typed
    where entity = 'notarial_acts' and column_number = 1 and label_l = 'akty notarialne razem'
),

notarial as (
    select
        'notarial_acts' as table_code,
        'acts' as measure_code,
        'total' as breakdown_code,
        'acts' as unit_code,
        cast(y.txt as integer) as period_year,
        v.*
    from notarial_header as h
    inner join typed as y
        on h.entity = y.entity and h.sheet_name = y.sheet_name and h.header_row = y.row_number
    inner join vals as v
        on
            h.entity = v.entity and h.sheet_name = v.sheet_name and v.row_number = h.header_row + 1
            and y.column_number = v.column_number
    where
        y.column_number >= 2 and regexp_matches(y.txt, '^(19|20)[0-9]{2}$')
        and not exists (
            select 1
            from typed as l
            where
                l.entity = h.entity and l.sheet_name = h.sheet_name and l.row_number = h.header_row + 1
                and l.column_number = 1
        )
),

unioned as (
    select * from caseload
    union all by name
    select * from duration
    union all by name
    select * from adoptions
    union all by name
    select * from notarial
)

select
    table_code,
    measure_code,
    breakdown_code,
    unit_code,
    period_year,
    value,
    value_text,
    cell_status,
    entity,
    sheet_name,
    row_number as source_row,
    column_number as source_column,
    source_version,
    raw_sha256
from unioned
