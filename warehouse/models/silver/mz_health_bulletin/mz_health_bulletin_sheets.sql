{# MZ / Centrum e-Zdrowia "Biuletyn Statystyczny" (register PLS-017, #2957): every sheet of the newest copy of each
   XLSX edition (bronze.mz_health_bulletin_cells) with what Silver does with it and why. Grain: entity x sheet_name.
   Source: the sheet's first two rows (title) and its header row (the first row whose first cell starts "Województw").

   table_kind comes from the title, never from the sheet name or number (2020 numbers its sheets 2.1-2.24, the others
   2.1.1-...; the 2021-2025 hospital sheet is titled "Tablica 4.4.1"):
   - licensed:          "Personel medyczny uprawniony do wykonywania zawodu według województw" (31 December), not
                        the tables by age and sex nor the pharmacist / laboratory diagnostician tables
   - employed:          "Personel pracujący w placówkach ochrony zdrowia według województw" (31 December, MZ-88)
   - hospitals:         "Szpitale stacjonarne ogólne według województw" (MZ-29)
   - hospital_activity: "Działalność szpitali stacjonarnych ogólnych z ruchem międzyoddziałowym" (MZ-29)
   Reference year: from the title (31.12.<year> or "w <year> roku); the year of a table kind in an edition is the year
   most of its tables state (the 2019 edition titles one of three "employed" tables 31.12.2019, the other two
   31.12.2018); a tie or a year outside edition year - 3 .. edition year - 1 gives none.

   status: modelled (read by mz_health_bulletin_tables); not_modelled (other tables, kept in Bronze; reason says
   which); rejected (a modelled table kind whose layout changed: no header row, a column outside the closed list,
   not exactly Poland and the 16 voivodeships, no reference year). A rejected sheet is left out of Silver and fails
   the accepted_values test of status, with the reason here: schema drift stops the build instead of thinning or
   misreading the series. #}
with specs as (
    {{ poc_mz_health_bulletin_specs() }}
),

regions as (
    {{ gus_regions() }}
),

cells as (
    select
        entity,
        sheet_name,
        row_number,
        column_number,
        source_version,
        raw_sha256,
        cast(regexp_extract(entity, '_([0-9]{4})$', 1) as integer) as edition_year,
        trim(regexp_replace(replace(replace(value_text, chr(160), ' '), '_', ' '), '\s+', ' ', 'g')) as txt
    from ({{ xlsx_newest_cells(source('mz_health_bulletin', 'mz_health_bulletin_cells')) }})
),

sheets as (
    select
        entity,
        sheet_name,
        edition_year,
        min(source_version) as source_version,
        min(raw_sha256) as raw_sha256,
        string_agg(txt, ' ' order by row_number, column_number) filter (where row_number <= 2) as title
    from cells
    group by all
),

header_rows as (
    select
        entity,
        sheet_name,
        min(row_number) as header_row
    from cells
    where column_number = 1 and regexp_matches(txt, '^(?i)województw')
    group by all
),

kinds as (
    select
        s.*,
        h.header_row,
        case
            when
                regexp_matches(s.title, '(?i)uprawnion\S* do wykonywania zawodu według województw')
                and not regexp_matches(s.title, '(?i)wieku|farmaceut|diagnost')
                then 'licensed'
            when regexp_matches(s.title, '(?i)personel pracujący w placówkach ochrony zdrowia\s*1?\s*według województw')
                then 'employed'
            when regexp_matches(s.title, '(?i)szpitale stacjonarne ogólne\s*1?\s*według województw') then 'hospitals'
            when regexp_matches(s.title, '(?i)działalność szpitali stacjonarnych ogólnych z ruchem międzyoddziałowym')
                then 'hospital_activity'
        end as table_kind,
        list_distinct(regexp_extract_all(coalesce(s.title, ''), '(?:19|20)[0-9]{2}')) as title_years
    from sheets as s
    left join header_rows as h on s.entity = h.entity and s.sheet_name = h.sheet_name
),

-- header cells of the table-kind sheets, each classed: a modelled column, a national rank, the "powrót" link, or
-- unknown
headers as (
    select
        k.entity,
        k.sheet_name,
        c.column_number,
        lower(trim(regexp_replace(c.txt, '\s*[0-9]+$', ''))) as header
    from kinds as k
    inner join cells as c
        on k.entity = c.entity and k.sheet_name = c.sheet_name and k.header_row = c.row_number
    where k.table_kind is not null and c.column_number > 1 and c.txt <> ''
),

header_classes as (
    select
        h.entity,
        h.sheet_name,
        h.header,
        case
            when count(s.header_regex) = 1 then 'measure'
            when count(s.header_regex) > 1 then 'ambiguous'
            when regexp_matches(h.header, 'miejsce w kr') then 'rank'
            when h.header = 'powrót' then 'link'
            else 'unknown'
        end as header_class
    from headers as h
    inner join kinds as k on h.entity = k.entity and h.sheet_name = k.sheet_name
    left join specs as s on k.table_kind = s.table_kind and regexp_matches(h.header, s.header_regex)
    group by h.entity, h.sheet_name, h.header
),

header_summary as (
    select
        entity,
        sheet_name,
        count(*) filter (where header_class = 'measure') as measure_columns,
        string_agg(header, '; ' order by header) filter (where header_class in ('unknown', 'ambiguous'))
            as unknown_headers,
        bool_and(
            header_class = 'link' or (header_class = 'unknown' and header in ('farmaceuci', 'diagności laboratoryjni'))
        )
        and count(*) filter (where header in ('farmaceuci', 'diagności laboratoryjni')) > 0 as several_years_layout
    from header_classes
    group by all
),

area_labels as (
    select
        c.entity,
        c.sheet_name,
        regexp_replace(lower(regexp_replace(c.txt, '^(?i)województwo\s+', '')), '\s*-\s*', '-', 'g') as label
    from cells as c
    inner join kinds as k
        on c.entity = k.entity and c.sheet_name = k.sheet_name and c.row_number > k.header_row
    where k.table_kind is not null and c.column_number = 1
),

area_summary as (
    select
        a.entity,
        a.sheet_name,
        count(*) as area_rows,
        count(distinct a.label) as areas
    from area_labels as a
    left join regions as r on a.label = r.area_name
    where a.label = 'polska' or r.area_name is not null
    group by all
),

classified as (
    select
        k.*,
        hs.measure_columns,
        hs.unknown_headers,
        coalesce(hs.several_years_layout, false) and k.table_kind = 'licensed' as several_years_layout,
        coalesce(a.area_rows, 0) as area_rows,
        coalesce(a.areas, 0) as areas,
        case when len(k.title_years) = 1 then cast(k.title_years[1] as integer) end as title_year
    from kinds as k
    left join header_summary as hs on k.entity = hs.entity and k.sheet_name = hs.sheet_name
    left join area_summary as a on k.entity = a.entity and k.sheet_name = a.sheet_name
),

kind_years as (
    select
        entity,
        table_kind,
        title_year,
        count(*) as n
    from classified
    where table_kind is not null and not several_years_layout and title_year is not null
    group by all
),

ranked_years as (
    select
        *,
        rank() over (partition by entity, table_kind order by n desc) as r
    from kind_years
),

edition_years as (
    select
        entity,
        table_kind,
        case when count(*) filter (where r = 1) = 1 then max(title_year) filter (where r = 1) end as majority_year
    from ranked_years
    group by all
),

judged as (
    select
        c.*,
        case
            when e.majority_year between c.edition_year - 3 and c.edition_year - 1 then e.majority_year
        end as period_year,
        case
            when c.table_kind is null then 'not a modelled table'
            when c.several_years_layout
                then 'licensed pharmacists or laboratory diagnosticians, several years per table (not modelled)'
            when c.header_row is null then 'no header row starting "Województw"'
            when c.unknown_headers is not null then 'column outside the closed list: ' || c.unknown_headers
            when coalesce(c.measure_columns, 0) = 0 then 'no modelled column'
            when c.area_rows <> 17 or c.areas <> 17
                then 'areas: ' || c.area_rows || ' rows, ' || c.areas || ' distinct of Poland and 16 voivodeships'
            when not coalesce(e.majority_year between c.edition_year - 3 and c.edition_year - 1, false)
                then
                    'no reference year (title years ' || coalesce(list_aggregate(c.title_years, 'string_agg', ','), '')
                    || ', majority ' || coalesce(cast(e.majority_year as varchar), 'none') || ')'
        end as reason
    from classified as c
    left join edition_years as e on c.entity = e.entity and c.table_kind = e.table_kind
)

select
    entity,
    edition_year,
    sheet_name,
    title,
    table_kind,
    title_year,
    period_year,
    coalesce(title_year, -1) <> coalesce(period_year, -1) and table_kind is not null as title_year_differs,
    header_row,
    measure_columns,
    unknown_headers,
    area_rows,
    case
        when reason is null then 'modelled'
        when table_kind is null or several_years_layout then 'not_modelled'
        else 'rejected'
    end as status,
    reason,
    source_version,
    raw_sha256
from judged
