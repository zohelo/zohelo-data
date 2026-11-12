{# MZ / Centrum e-Zdrowia "Biuletyn Statystyczny" (register PLS-017, #2957): the modelled tables of every XLSX edition
   (2019-), Poland and the 16 voivodeships, from the newest copy of each edition (bronze.mz_health_bulletin_cells).
   Grain: edition_year x table_kind x profession_code x measure_code x area_name (one reference year per edition and
   table kind). Every other sheet stays in Bronze as cells, not modelled (docs/sources/mz_health_bulletin.md).

   Tables are found by title, never by sheet name or number (2020 numbers its sheets 2.1-2.24, the others 2.1.1-...;
   the 2021-2025 hospital sheet is titled "Tablica 4.4.1"):
   - licensed:          "Personel medyczny uprawniony do wykonywania zawodu według województw" (stock on 31 December;
                        doctors, dentists, nurses, midwives; register counts of the chambers or MZ-10A/B), not the
                        tables by age and sex nor the pharmacist / laboratory diagnostician tables (several years)
   - employed:          "Personel pracujący w placówkach ochrony zdrowia według województw" (persons by primary
                        workplace on 31 December, MZ-88, without MSWiA and MS; pharmacists without pharmacies)
   - hospitals:         "Szpitale stacjonarne ogólne według województw" (general hospitals and their beds, MZ-29)
   - hospital_activity: "Działalność szpitali stacjonarnych ogólnych z ruchem międzyoddziałowym" (inpatients treated
                        in the year = cases, patient-days in thousands, average stay, bed use; MZ-29)
   Columns are found by the header row (the row whose first cell starts "Województw"), header text with '_' read as
   a space and a trailing footnote number dropped, matched in full to the closed list below. The national ranks
   ("miejsce w kraju") are not modelled.
   Reference year: read from each table title (31.12.<year> or "w <year> roku"); the year of a table kind in an
   edition is the year most of its tables state (the 2019 edition titles one of three "employed" tables 31.12.2019,
   the other two 31.12.2018: the majority wins, title_year keeps what the title says); a tie, or a year outside edition
   year - 3 .. edition year - 1, gives no year and fails the not_null test. Values as published (D33: rates per
   10,000 population as printed, patient-days in thousands); symbols are never zeros: '-' dash, '.' missing, 'x'
   not_applicable (no value), any other text unparsed (accepted_values fails). is_latest_edition marks the newest
   edition holding a key x reference year: a reference year printed in two editions keeps both rows here, Gold
   reads the latest edition only. #}
{% set professions = [
    ('lekarze', 'doctors'), ('lekarze dentyści', 'dentists'), ('farmaceuci', 'pharmacists'),
    ('diagności laboratoryjni', 'lab_diagnosticians'), ('pielęgniarki', 'nurses'), ('położne', 'midwives'),
    ('fizjoterapeuci', 'physiotherapists'), ('ratownicy medyczni', 'paramedics'), ('dietetycy', 'dietitians'),
] %}
{% set specs = [] %}
{% for kind in ['licensed', 'employed'] %}{% for p in professions %}
    {% do specs.append((kind, '^' ~ p[0] ~ ' liczba bezw[a-ząćęłńóśźż]*$', p[1], 'count', 'persons')) %}
    {% do specs.append((kind, '^' ~ p[0] ~ ' wskaźnik$', p[1], 'per_10k', 'per_10k_population')) %}
{% endfor %}{% endfor %}
{% do specs.extend([
    ('hospitals', '^szpitale ogółem$', 'hospitals', 'count', 'hospitals'),
    ('hospitals', '^łóżka ogółem$', 'beds', 'count', 'beds'),
    ('hospitals', '^łóżka wskaźnik$', 'beds', 'per_10k', 'per_10k_population'),
    ('hospital_activity', '^leczeni w ciągu roku ogółem$', 'inpatients', 'count', 'cases'),
    ('hospital_activity', '^leczeni na 1 łóżko wskaźn[a-z]*$', 'inpatients', 'per_bed', 'cases_per_bed'),
    ('hospital_activity', '^osobodni w tysiącach$', 'patient_days', 'count', 'thousand_patient_days'),
    ('hospital_activity', '^przeciętny pobyt chorego wskaźnik w dniach$', 'length_of_stay', 'mean', 'days'),
    ('hospital_activity', '^wykorzystanie łóżka wskaźnik w dniach$', 'bed_occupancy', 'days', 'days'),
    ('hospital_activity', '^wykorzystanie łóżka wskaźnik w %$', 'bed_occupancy', 'pct', 'pct'),
]) %}
with specs as (
    select * from (
        values
        {%- for s in specs %}
            ('{{ s[0] }}', '{{ s[1] }}', '{{ s[2] }}', '{{ s[3] }}', '{{ s[4] }}'){{ "," if not loop.last }}
        {%- endfor %}
    ) as t (table_kind, header_regex, profession_code, measure_code, unit_code)
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
        cell_type,
        value_text,
        source_version,
        raw_sha256,
        cast(regexp_extract(entity, '_([0-9]{4})$', 1) as integer) as edition_year,
        trim(regexp_replace(replace(replace(value_text, chr(160), ' '), '_', ' '), '\s+', ' ', 'g')) as txt
    from ({{ xlsx_newest_cells(source('mz_health_bulletin', 'mz_health_bulletin_cells')) }})
),

titles as (
    select
        entity,
        sheet_name,
        edition_year,
        string_agg(txt, ' ' order by row_number, column_number) as title
    from cells
    where row_number <= 2
    group by all
),

tables as (
    select
        *,
        case
            when
                regexp_matches(title, '(?i)uprawnion\S* do wykonywania zawodu według województw')
                and not regexp_matches(title, '(?i)wieku|farmaceut|diagnost')
                then 'licensed'
            when regexp_matches(title, '(?i)personel pracujący w placówkach ochrony zdrowia\s*1?\s*według województw')
                then 'employed'
            when regexp_matches(title, '(?i)szpitale stacjonarne ogólne\s*1?\s*według województw') then 'hospitals'
            when regexp_matches(title, '(?i)działalność szpitali stacjonarnych ogólnych z ruchem międzyoddziałowym')
                then 'hospital_activity'
        end as table_kind,
        list_distinct(regexp_extract_all(title, '(?:19|20)[0-9]{2}')) as title_years
    from titles
),

kind_tables as (
    select
        *,
        case when len(title_years) = 1 then cast(title_years[1] as integer) end as title_year
    from tables
    where table_kind is not null
),

kind_years as (
    select
        entity,
        table_kind,
        title_year,
        count(*) as n
    from kind_tables
    where title_year is not null
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
        case when count(*) filter (where r = 1) = 1 then max(title_year) filter (where r = 1) end as period_year
    from ranked_years
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

headers as (
    select
        c.entity,
        c.sheet_name,
        c.column_number,
        lower(trim(regexp_replace(c.txt, '\s*[0-9]+$', ''))) as header
    from cells as c
    inner join header_rows as h
        on c.entity = h.entity and c.sheet_name = h.sheet_name and c.row_number = h.header_row
    where c.column_number > 1
),

columns_matched as (
    select
        t.entity,
        t.sheet_name,
        t.edition_year,
        t.title,
        t.title_year,
        t.table_kind,
        h.column_number,
        h.header,
        s.profession_code,
        s.measure_code,
        s.unit_code
    from kind_tables as t
    inner join headers as h on t.entity = h.entity and t.sheet_name = h.sheet_name
    inner join specs as s on t.table_kind = s.table_kind and regexp_matches(h.header, s.header_regex)
),

labels as (
    select
        c.entity,
        c.sheet_name,
        c.row_number,
        regexp_replace(lower(regexp_replace(c.txt, '^(?i)województwo\s+', '')), '\s*-\s*', '-', 'g') as label
    from cells as c
    inner join header_rows as h
        on c.entity = h.entity and c.sheet_name = h.sheet_name and c.row_number > h.header_row
    where c.column_number = 1
),

-- the voivodeship names of macro gus_regions (warehouse/macros/gus_tables.sql) and the Poland row
area_rows as (
    select
        l.entity,
        l.sheet_name,
        l.row_number,
        coalesce(r.area_name, 'polska') as area_name
    from labels as l
    left join regions as r on l.label = r.area_name
    where l.label = 'polska' or r.area_name is not null
),

picked as (
    select
        m.*,
        a.area_name,
        a.row_number,
        c.cell_type,
        c.value_text,
        c.txt,
        c.source_version,
        c.raw_sha256,
        e.period_year as resolved_year
    from columns_matched as m
    inner join area_rows as a on m.entity = a.entity and m.sheet_name = a.sheet_name
    inner join cells as c
        on
            a.entity = c.entity and a.sheet_name = c.sheet_name and a.row_number = c.row_number
            and m.column_number = c.column_number
    left join edition_years as e on m.entity = e.entity and m.table_kind = e.table_kind
),

typed as (
    select
        *,
        case
            when resolved_year between edition_year - 3 and edition_year - 1 then resolved_year
        end as period_year,
        case
            when cell_type = 'n' and try_cast(value_text as double) is not null then 'ok'
            when regexp_matches(txt, '^-?[0-9]+([.,][0-9]+)?$') then 'ok'
            when txt in ('-', '–', '—') then 'dash'
            when txt = '.' then 'missing'
            when lower(txt) = 'x' then 'not_applicable'
            else 'unparsed'
        end as cell_status
    from picked
)

select
    edition_year,
    table_kind,
    profession_code,
    measure_code,
    unit_code,
    area_name,
    period_year,
    case when cell_status = 'ok' then cast(replace(txt, ',', '.') as double) end as value,
    value_text,
    cell_status,
    edition_year = max(edition_year) over (
        partition by table_kind, profession_code, measure_code, area_name, period_year
    ) as is_latest_edition,
    title_year,
    coalesce(title_year, -1) <> coalesce(period_year, -1) as title_year_differs,
    entity,
    sheet_name,
    title,
    header,
    row_number,
    column_number,
    source_version,
    raw_sha256
from typed
