{# OTOMOTO Insights (register CD-034, #3264): the monthly series of the market report chosen for Gold, from the newest
   copy of every edition (bronze.otomoto_cells), Poland, by segment. A segment is a tab of the report headed
   "Ogłoszenia na OTOMOTO (<segment>)" (Samochody używane, Samochody nowe, Motocykle, Maszyny ciężkie):
   - active_listings: the chart of a segment tab that has the title "Średnia dzienna liczba aktywnych ogłoszeń" and the
     header row "rok | <year> | <year> | r/r": rows are months (sty ... gru), the year columns the years (the r/r
     column is a derived change and is not read); unit: listings, as published;
   - users_index, buyers_index: the chart of a segment tab titled "Statystyki ruchu (jako % okresu bazowego 01.2025)"
     whose header is "Miesiąc | Miesięczna liczba użytkowników [| Miesięczna liczba kupujących]": rows YYYY-MM; unit:
     % of January 2025 (January 2025 = 100). Heavy machinery has users only; used cars have no traffic chart.
   Every edition repeats the months of the current and the previous year, possibly revised: for each series and month
   the newest edition that holds it is kept (the edition is the version). A chart found twice in one edition repeats a
   key and fails the unique test; a month label outside sty ... gru or YYYY-MM fails the build. value is the published
   text read as a number (spaces dropped, decimal comma); other text keeps value null, value_status 'not_numeric',
   never 0; empty cells are not in Bronze (no value). Grain: measure_code x segment_label x period_month. #}
with cells as (
    select *
    from {{ source('otomoto', 'otomoto_cells') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

texts as (
    select distinct
        edition,
        tab_number,
        value
    from cells
    where record_kind = 'text'
),

segment_tabs as (
    select
        edition,
        tab_number,
        regexp_extract(value, '^Ogłoszenia na OTOMOTO \((.+)\)$', 1) as segment_label
    from texts
    where regexp_full_match(value, 'Ogłoszenia na OTOMOTO \(.+\)')
),

titled as (
    select
        s.edition,
        s.tab_number,
        s.segment_label,
        t.value as chart_title
    from segment_tabs as s
    inner join texts as t on s.edition = t.edition and s.tab_number = t.tab_number
    where t.value in ('Średnia dzienna liczba aktywnych ogłoszeń', 'Statystyki ruchu (jako % okresu bazowego 01.2025)')
),

grid as (
    select *
    from cells
    where record_kind = 'cell'
),

headers as (
    select
        edition,
        tab_number,
        element_id,
        sheet_number,
        string_agg(value, '|' order by column_number) as header
    from grid
    where row_number = 1
    group by all
),

charts as (
    select
        h.edition,
        h.element_id,
        h.sheet_number,
        t.segment_label,
        t.chart_title
    from headers as h
    inner join titled as t on h.edition = t.edition and h.tab_number = t.tab_number
    where
        (
            t.chart_title = 'Średnia dzienna liczba aktywnych ogłoszeń'
            and regexp_full_match(h.header, 'rok\|[0-9]{4}\|[0-9]{4}\|r/r')
        )
        or (
            t.chart_title = 'Statystyki ruchu (jako % okresu bazowego 01.2025)'
            and h.header in (
                'Miesiąc|Miesięczna liczba użytkowników',
                'Miesiąc|Miesięczna liczba użytkowników|Miesięczna liczba kupujących'
            )
        )
),

values_read as (
    select
        c.edition,
        c.segment_label,
        c.chart_title,
        v.row_number,
        v.value as value_text,
        h.value as column_header,
        m.value as row_label,
        v.raw_sha256,
        v.source_version
    from charts as c
    inner join grid as v
        on c.edition = v.edition and c.element_id = v.element_id and c.sheet_number = v.sheet_number
    inner join grid as h
        on
            v.edition = h.edition and v.element_id = h.element_id and v.sheet_number = h.sheet_number
            and v.column_number = h.column_number and h.row_number = 1
    left join grid as m
        on
            v.edition = m.edition and v.element_id = m.element_id and v.sheet_number = m.sheet_number
            and v.row_number = m.row_number and m.column_number = 1
    where
        v.row_number > 1
        and v.column_number > 1
        and (
            (c.chart_title = 'Średnia dzienna liczba aktywnych ogłoszeń' and regexp_full_match(h.value, '[0-9]{4}'))
            or (
                c.chart_title = 'Statystyki ruchu (jako % okresu bazowego 01.2025)'
                and h.value in ('Miesięczna liczba użytkowników', 'Miesięczna liczba kupujących')
            )
        )
),

typed as (
    select
        edition,
        segment_label,
        case
            when column_header = 'Miesięczna liczba użytkowników' then 'users_index'
            when column_header = 'Miesięczna liczba kupujących' then 'buyers_index'
            else 'active_listings'
        end as measure_code,
        case when chart_title = 'Średnia dzienna liczba aktywnych ogłoszeń' then chart_title else column_header end
            as series_label,
        case
            when
                chart_title = 'Statystyki ruchu (jako % okresu bazowego 01.2025)'
                and regexp_full_match(row_label, '20[0-9]{2}-(0[1-9]|1[0-2])')
                then cast(row_label || '-01' as date)
            when chart_title = 'Średnia dzienna liczba aktywnych ogłoszeń'
                then make_date(
                    cast(column_header as integer),
                    case row_label
                        when 'sty' then 1 when 'lut' then 2 when 'mar' then 3 when 'kwi' then 4 when 'maj' then 5
                        when 'cze' then 6 when 'lip' then 7 when 'sie' then 8 when 'wrz' then 9 when 'wrze' then 9
                        when 'paź' then 10 when 'lis' then 11 when 'gru' then 12
                        else error('otomoto: month label ' || coalesce(row_label, 'null') || ' in edition ' || edition)
                    end,
                    1
                )
            else error('otomoto: month label ' || coalesce(row_label, 'null') || ' in edition ' || edition)
        end as period_month,
        value_text,
        replace(replace(value_text, ' ', ''), chr(160), '') as compact,
        case when chart_title = 'Średnia dzienna liczba aktywnych ogłoszeń' then 'listings' else 'jan2025_100' end
            as unit_code,
        raw_sha256,
        source_version
    from values_read
)

select
    measure_code,
    segment_label,
    period_month,
    series_label,
    case when regexp_full_match(compact, '-?[0-9]+([.,][0-9]+)?') then cast(replace(compact, ',', '.') as double) end
        as value,
    value_text,
    case when regexp_full_match(compact, '-?[0-9]+([.,][0-9]+)?') then 'ok' else 'not_numeric' end as value_status,
    unit_code,
    edition,
    raw_sha256,
    source_version
from typed
qualify rank() over (partition by measure_code, segment_label, period_month order by edition desc) = 1
