{# KNF insurance and pension statistics (bronze.knf_insurance_cells, newest copy of every release workbook): one row
   per series × period × table × item × provider × measure, values as published (no rescaling).

   bulletin  Quarterly bulletin "Rynek ubezpieczeń", part A, the four tables found by their English title (row 1-3):
             Premiums / Claims of Life / Non-Life Insurance Companies (A.3, A.7, A.14, A.18 in 2026): one row per insurer
             and the total ("Razem / Total"); measures from the header row ("Wyszczególnienie" in column 1); period from
             "za okres / for: 01.01.YYYY - DD.MM.YYYY" (year to date, frequency Q, period = the quarter of the end date)
   ofe       OFE monthly workbook, sheets Aktywa netto, Członkowie, Składki, Jednostka rozrachunkowa: one row per fund and
             the total ("Razem", "Średnia ważona"); header row = column B "Otwarty fundusz emerytalny"; period from
             "Stan na dzień: / Dane miesięczne: YYYY-MM-DD" (frequency M)
   ike/ikze  "<IKE|IKZE>_dane liczbowe": one row per numbered item (column B) × provider group (row above the measure
             row; columns before the first group = all providers, '_T') × measure (Liczba (w szt.) / Wartość (w tys. zł));
             period from "Stan na dzień: YYYY-12-31" (frequency A)

   Only number cells are values: text in a value position ("x", "-", "b.d.") is not a number and is left out, never 0.
   measure_code is null for a header the model does not know (the not_null test then fails); unit_label is the unit as
   printed in the header ("w tys. zł", "w PLN", "w szt."), unit_code its unit.csv key. #}
with cells as (
    select
        *,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text,
        try_cast(value_text as double) as number_value,
        split_part(entity, '_', 1) as series
    from ({{ xlsx_newest_cells(source('knf_insurance', 'knf_insurance_cells')) }}) as newest_cells
),

-- bulletin part A ---------------------------------------------------------------------------------------------------
b_tables as (
    select
        entity,
        sheet_name,
        regexp_extract(text, '^Table (A\.[0-9]+)\.', 1) as table_code,
        text as table_title,
        case when regexp_matches(text, 'Non-Life') then 'non_life' else 'life' end as business_line,
        case when regexp_matches(text, '\. Premiums of') then 'premiums' else 'claims' end as table_kind
    from cells
    where
        series = 'bulletin' and ends_with(entity, '_a') and column_number = 1 and row_number <= 3
        and regexp_matches(text, '^Table A\.[0-9]+\. (Premiums|Claims) of (Life|Non-Life) Insurance Companies$')
),

b_periods as (
    select
        c.entity,
        c.sheet_name,
        cast(strptime(regexp_extract(c.text, '([0-9.]{10}) ?- ?[0-9.]{10}', 1), '%d.%m.%Y') as date) as period_from,
        cast(strptime(regexp_extract(c.text, '[0-9.]{10} ?- ?([0-9.]{10})', 1), '%d.%m.%Y') as date) as period_end
    from cells as c
    inner join b_tables as t on c.entity = t.entity and c.sheet_name = t.sheet_name
    where c.column_number = 1 and c.row_number <= 4 and starts_with(c.text, 'za okres')
),

b_header as (
    select
        c.entity,
        c.sheet_name,
        min(c.row_number) as header_row
    from cells as c
    inner join b_tables as t on c.entity = t.entity and c.sheet_name = t.sheet_name
    where c.column_number = 1 and starts_with(c.text, 'Wyszczególnienie')
    group by all
),

b_measures as (
    select
        c.entity,
        c.sheet_name,
        c.column_number,
        c.text as measure_label,
        case
            when c.text like 'Składka przypisana brutto%' then 'gross_written_premium'
            when c.text like 'Składka zarobiona na udziale własnym%' then 'earned_premium_net'
            when c.text like 'Odszkodowania i świadczenia wypłacone brutto%' then 'claims_paid_gross'
            when c.text like 'Odszkodowania i świadczenia* %' then 'claims_incurred_net'
        end as measure_code,
        nullif(regexp_extract(c.text, '\((w [^)]*)\)', 1), '') as unit_label
    from cells as c
    inner join b_header as h on c.entity = h.entity and c.sheet_name = h.sheet_name and c.row_number = h.header_row
    where c.column_number >= 2
),

bulletin as (
    select
        'bulletin' as series,
        t.entity,
        t.table_code,
        t.table_title,
        i.text as item_label,
        cast(null as varchar) as item_code,
        i.text in ('Razem / Total', 'Razem') as is_total,
        '_T' as provider,
        t.business_line,
        m.measure_code,
        m.measure_label,
        m.unit_label,
        case when m.unit_label = 'w tys. zł' then 'thousand_pln' end as unit_code,
        'Q' as frequency,
        p.period_from,
        p.period_end,
        make_date(year(p.period_end), 3 * quarter(p.period_end) - 2, 1) as period_start,
        year(p.period_end) || '-Q' || quarter(p.period_end) as period_label,
        v.number_value as value,
        i.row_number as source_row,
        v.raw_sha256,
        v.source_version,
        v.ingested_at
    from b_tables as t
    inner join b_header as h on t.entity = h.entity and t.sheet_name = h.sheet_name
    inner join b_periods as p on t.entity = p.entity and t.sheet_name = p.sheet_name
    inner join cells as i
        on
            t.entity = i.entity and t.sheet_name = i.sheet_name and i.column_number = 1
            and h.header_row < i.row_number
    inner join b_measures as m on t.entity = m.entity and t.sheet_name = m.sheet_name
    inner join cells as v
        on
            i.entity = v.entity and i.sheet_name = v.sheet_name and i.row_number = v.row_number
            and m.column_number = v.column_number
    where v.cell_type = 'n' and v.number_value is not null
),

-- OFE monthly ------------------------------------------------------------------------------------------------------
o_sheets as (
    select
        entity,
        sheet_name,
        min(row_number) as header_row
    from cells
    where
        series = 'ofe' and sheet_name in ('Aktywa netto', 'Członkowie', 'Składki', 'Jednostka rozrachunkowa')
        and column_number = 2 and text = 'Otwarty fundusz emerytalny'
    group by all
),

o_periods as (
    select
        c.entity,
        c.sheet_name,
        cast(regexp_extract(c.text, '([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) as date) as period_end
    from cells as c
    inner join o_sheets as s on c.entity = s.entity and c.sheet_name = s.sheet_name
    where
        c.row_number < s.header_row
        and regexp_matches(c.text, '^(Stan na dzień|Dane miesięczne): *[0-9]{4}-[0-9]{2}-[0-9]{2}$')
),

o_measures as (
    select
        c.entity,
        c.sheet_name,
        c.column_number,
        c.text as measure_label,
        case
            when c.text like 'Wartość aktywów netto%' then 'net_assets'
            when c.text = 'Liczba członków' then 'members'
            when c.text like 'Kwota składek%' then 'contributions'
            when c.text like 'Odsetki%' then 'contribution_interest'
            when c.text = 'Liczba składek' then 'contributions_count'
            when c.text like 'Wartość jednostki rozrachunkowej%' then 'unit_value'
        end as measure_code,
        nullif(regexp_extract(c.text, '\((w [^)]*)\)', 1), '') as unit_label
    from cells as c
    inner join o_sheets as s on c.entity = s.entity and c.sheet_name = s.sheet_name and c.row_number = s.header_row
    where c.column_number >= 3
),

ofe as (
    select
        'ofe' as series,
        s.entity,
        s.sheet_name as table_code,
        s.sheet_name as table_title,
        i.text as item_label,
        cast(null as varchar) as item_code,
        i.text in ('Razem', 'Razem:', 'Średnia ważona') as is_total,
        '_T' as provider,
        'pension' as business_line,
        m.measure_code,
        m.measure_label,
        m.unit_label,
        case
            when m.unit_label = 'w PLN' then 'pln'
            when m.measure_code = 'members' then 'persons'
            when m.measure_code = 'contributions_count' then 'number'
        end as unit_code,
        'M' as frequency,
        cast(date_trunc('month', p.period_end) as date) as period_from,
        p.period_end,
        cast(date_trunc('month', p.period_end) as date) as period_start,
        strftime(p.period_end, '%Y-%m') as period_label,
        v.number_value as value,
        i.row_number as source_row,
        v.raw_sha256,
        v.source_version,
        v.ingested_at
    from o_sheets as s
    inner join o_periods as p on s.entity = p.entity and s.sheet_name = p.sheet_name
    inner join cells as i
        on
            s.entity = i.entity and s.sheet_name = i.sheet_name and i.column_number = 2
            and s.header_row < i.row_number
    inner join o_measures as m on s.entity = m.entity and s.sheet_name = m.sheet_name
    inner join cells as v
        on
            i.entity = v.entity and i.sheet_name = v.sheet_name and i.row_number = v.row_number
            and m.column_number = v.column_number
    where v.cell_type = 'n' and v.number_value is not null
),

-- IKE / IKZE yearly ------------------------------------------------------------------------------------------------
k_sheets as (
    select
        entity,
        sheet_name,
        split_part(entity, '_', 1) as series,
        min(row_number) as measure_row
    from cells
    where series in ('ike', 'ikze') and ends_with(sheet_name, '_dane liczbowe') and text = 'Liczba (w szt.)'
    group by all
),

k_periods as (
    select
        c.entity,
        c.sheet_name,
        cast(regexp_extract(c.text, '([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) as date) as period_end
    from cells as c
    inner join k_sheets as s on c.entity = s.entity and c.sheet_name = s.sheet_name
    where c.row_number < s.measure_row and regexp_matches(c.text, '^Stan na dzień: *[0-9]{4}-[0-9]{2}-[0-9]{2}$')
),

k_columns as (
    select
        c.entity,
        c.sheet_name,
        c.column_number,
        c.text as measure_label,
        case c.text when 'Liczba (w szt.)' then 'count' when 'Wartość (w tys. zł)' then 'value' end as measure_code,
        nullif(regexp_extract(c.text, '\((w [^)]*)\)', 1), '') as unit_label,
        (
            select arg_max(g.text, g.column_number)
            from cells as g
            where
                g.entity = c.entity and g.sheet_name = c.sheet_name and g.row_number = s.measure_row - 1
                and g.column_number <= c.column_number and g.column_number >= 3
        ) as provider_label
    from cells as c
    inner join k_sheets as s on c.entity = s.entity and c.sheet_name = s.sheet_name and c.row_number = s.measure_row
    where c.column_number >= 3
),

ike as (
    select
        s.series,
        s.entity,
        s.sheet_name as table_code,
        s.sheet_name as table_title,
        i.text as item_label,
        rtrim(regexp_extract(i.text, '^([0-9]+(\.[0-9]+)*\.?([a-z]\.?)?)', 1), '.') as item_code,
        k.provider_label is null as is_total,
        case
            when k.provider_label is null then '_T'
            when k.provider_label = 'zakłady ubezpieczeń' then 'insurers'
            when k.provider_label = 'fundusze inwestycyjne zarządzane przez TFI' then 'investment_funds'
            when k.provider_label = 'podmioty prowadzące działalność maklerską' then 'brokers'
            when k.provider_label = 'banki' then 'banks'
            when
                k.provider_label = 'dobrowolne fundusze emerytalne zarządzane przez PTE'
                then 'voluntary_pension_funds'
        end as provider,
        'pension' as business_line,
        k.measure_code,
        k.measure_label,
        k.unit_label,
        case k.unit_label when 'w szt.' then 'pieces' when 'w tys. zł' then 'thousand_pln' end as unit_code,
        'A' as frequency,
        make_date(year(p.period_end), 1, 1) as period_from,
        p.period_end,
        make_date(year(p.period_end), 1, 1) as period_start,
        cast(year(p.period_end) as varchar) as period_label,
        v.number_value as value,
        i.row_number as source_row,
        v.raw_sha256,
        v.source_version,
        v.ingested_at
    from k_sheets as s
    inner join k_periods as p on s.entity = p.entity and s.sheet_name = p.sheet_name
    inner join cells as i
        on
            s.entity = i.entity and s.sheet_name = i.sheet_name and i.column_number = 2
            and s.measure_row < i.row_number
    inner join k_columns as k on s.entity = k.entity and s.sheet_name = k.sheet_name
    inner join cells as v
        on
            i.entity = v.entity and i.sheet_name = v.sheet_name and i.row_number = v.row_number
            and k.column_number = v.column_number
    where v.cell_type = 'n' and v.number_value is not null
)

select * from bulletin
union all
select * from ofe
union all
-- KNF prints 0 in the value column of IKE/IKZE rows it does not value (accounts opened, rows by sex and age):
-- a 0 value next to a positive count of the same row and provider is that marker, not a value, and is left out
select * from ike
qualify not (
    measure_code = 'value' and value = 0
    and max(case when measure_code = 'count' then value end) over (partition by entity, source_row, provider) > 0
)
