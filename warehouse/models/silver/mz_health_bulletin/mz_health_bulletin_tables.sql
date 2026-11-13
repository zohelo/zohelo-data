{# MZ / Centrum e-Zdrowia "Biuletyn Statystyczny" (register PLS-017, #2957): the values of the four modelled tables of
   every XLSX edition (2019-), Poland and the 16 voivodeships, from the newest copy of each edition
   (bronze.mz_health_bulletin_cells). Grain: edition_year x table_kind x profession_code x measure_code x area_name
   (one reference year per edition and table kind). Source: the sheets with status 'modelled' in
   mz_health_bulletin_sheets (table kind, header row, reference year found there); every other sheet stays in Bronze.

   Columns: the header-row text matched in full to the closed list of poc_mz_health_bulletin_specs()
   (warehouse/macros/sources/mz_health_bulletin.sql); the national ranks ("miejsce w kraju") are not modelled. Rows:
   the first-column label matched to "Polska" or a voivodeship name (gus_regions), never a row position.
   Values as published (D33: rates per 10,000 population as printed, patient-days in thousands); symbols are never
   zeros: '-' dash, '.' missing, 'x' not_applicable (no value), any other text unparsed (accepted_values fails).
   repeated: a sheet whose modelled cells equal, cell for cell, a sheet of the same table kind in an older edition
   with another reference year (the 2021 edition's Tablica 2.2.4, titled 31.12.2020, repeats the 2019 edition's
   31.12.2018 table to the last decimal of its rates): its values are not an observation of its reference year, so
   value is null and repeated_from names the older sheet. is_latest_edition marks the newest edition holding a key x
   reference year: a reference year printed in two editions keeps both rows here, Gold reads the latest only. #}
with specs as (
    {{ poc_mz_health_bulletin_specs() }}
),

regions as (
    {{ gus_regions() }}
),

sheets as (
    select *
    from {{ ref('mz_health_bulletin_sheets') }}
    where status = 'modelled'
),

cells as (
    select
        c.entity,
        c.sheet_name,
        c.row_number,
        c.column_number,
        c.cell_type,
        c.value_text,
        c.source_version,
        c.raw_sha256,
        trim(regexp_replace(replace(replace(c.value_text, chr(160), ' '), '_', ' '), '\s+', ' ', 'g')) as txt
    from ({{ xlsx_newest_cells(source('mz_health_bulletin', 'mz_health_bulletin_cells')) }}) as c
    inner join sheets as s on c.entity = s.entity and c.sheet_name = s.sheet_name
),

columns_matched as (
    select
        s.entity,
        s.sheet_name,
        s.edition_year,
        s.title,
        s.title_year,
        s.period_year,
        s.table_kind,
        s.header_row,
        c.column_number,
        lower(trim(regexp_replace(c.txt, '\s*[0-9]+$', ''))) as header,
        p.profession_code,
        p.measure_code,
        p.unit_code
    from sheets as s
    inner join cells as c
        on s.entity = c.entity and s.sheet_name = c.sheet_name and s.header_row = c.row_number
    inner join specs as p
        on
            s.table_kind = p.table_kind
            and regexp_matches(lower(trim(regexp_replace(c.txt, '\s*[0-9]+$', ''))), p.header_regex)
    where c.column_number > 1
),

-- the voivodeship names of macro gus_regions (warehouse/macros/gus_tables.sql) and the Poland row
area_rows as (
    select
        c.entity,
        c.sheet_name,
        c.row_number,
        coalesce(r.area_name, 'polska') as area_name
    from cells as c
    inner join sheets as s
        on c.entity = s.entity and c.sheet_name = s.sheet_name and c.row_number > s.header_row
    left join regions as r
        on regexp_replace(lower(regexp_replace(c.txt, '^(?i)województwo\s+', '')), '\s*-\s*', '-', 'g') = r.area_name
    where
        c.column_number = 1
        and (lower(c.txt) = 'polska' or r.area_name is not null)
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
        c.raw_sha256
    from columns_matched as m
    inner join area_rows as a on m.entity = a.entity and m.sheet_name = a.sheet_name
    inner join cells as c
        on
            a.entity = c.entity and a.sheet_name = c.sheet_name and a.row_number = c.row_number
            and m.column_number = c.column_number
),

signatures as (
    select
        entity,
        sheet_name,
        edition_year,
        table_kind,
        period_year,
        string_agg(
            profession_code || '.' || measure_code || '.' || area_name || '=' || value_text, '|'
            order by profession_code, measure_code, area_name
        ) as signature
    from picked
    group by all
),

repeats as (
    select
        n.entity,
        n.sheet_name,
        min(o.entity || ' ' || o.sheet_name) as repeated_from
    from signatures as n
    inner join signatures as o
        on
            n.table_kind = o.table_kind and n.signature = o.signature
            and n.edition_year > o.edition_year and n.period_year <> o.period_year
    group by all
),

typed as (
    select
        p.*,
        r.repeated_from,
        case
            when r.repeated_from is not null then 'repeated'
            when p.cell_type = 'n' and try_cast(p.value_text as double) is not null then 'ok'
            when regexp_matches(p.txt, '^-?[0-9]+([.,][0-9]+)?$') then 'ok'
            when p.txt in ('-', '–', '—') then 'dash'
            when p.txt = '.' then 'missing'
            when lower(p.txt) = 'x' then 'not_applicable'
            else 'unparsed'
        end as cell_status
    from picked as p
    left join repeats as r on p.entity = r.entity and p.sheet_name = r.sheet_name
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
    repeated_from,
    edition_year = max(edition_year) over (
        partition by table_kind, profession_code, measure_code, area_name, period_year
    ) as is_latest_edition,
    title_year,
    entity,
    sheet_name,
    title,
    header,
    row_number,
    column_number,
    source_version,
    raw_sha256
from typed
