{# ZUS statistical portal (psz.zus.pl, register PLS-009): the curated XLSX tables (newest copy of each family ×
   period, bronze.zus_psz_cells) as one row per family × period × area × sex × breakdown × measure.
   Every table is a report: a header row (column A starts with the family's header text), then one row per item
   (column A = label) with numbers under the header's columns. Families (ingestion/to_landing/zus_psz):
     pensioners_dec        old-age pensioners in December (A), Poland, by age group (rows) and sex (columns)
     pension_amount_dec    average old-age pension in December (A), PLN, by sex: the row "Przeciętna wysokość na
                           osobę - w zł" only (the amount distribution in percent is not modelled)
     insured_pension       insured persons in pension insurance at the quarter end (Q), by age group and sex
     insured_sickness_voiv insured persons in sickness insurance at the quarter end (Q), Poland and voivodeships
                           (powiat rows are left out)
     insured_health        insured persons in health insurance per month (M), Poland; the month is the row's place in
                           the year file (the 2026 file labels April "VI"), the label's year must match the file's
     foreign_insured       foreign citizens in pension insurance at the month end (M), all and Ukrainian citizens
     payers_voiv           active contribution payers at the quarter end (Q), Poland and voivodeships, by payer type
                           (TOTAL, SELF = a natural person paying only for themselves, and insured-count classes)
     absence_voiv          sick-leave days and medical certificates in the quarter (Q), Poland and voivodeships
   area_code: '00' = Poland (the "Ogółem" row, which also counts persons ZUS could not assign to a voivodeship), '_U'
   = the row "nieustalone województwo" (voivodeship not established; absence table), else the TERYT voivodeship code,
   matched by the official voivodeship name (ZUS gives names only; spaces around the hyphen ignored). Cells that are not
   numbers are left out. Values as published. #}
with cells as (
    select
        entity,
        row_number,
        column_number,
        cell_type,
        number_format,
        value_text,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text,
        regexp_replace(entity, '_20[0-9]{2}(_q[1-4])?$', '') as family,
        cast(regexp_extract(entity, '_(20[0-9]{2})', 1) as integer) as file_year,
        try_cast(nullif(regexp_extract(entity, '_q([1-4])$', 1), '') as integer) as file_quarter,
        raw_sha256,
        source_version,
        ingested_at
    from ({{ xlsx_newest_cells(source('zus_psz', 'zus_psz_cells')) }}) as newest_cells
),

-- the header text of each family; the same text as FAMILIES[...]['anchor'] in ingestion/to_landing/zus_psz, which
-- Bronze requires in every workbook (tests/zus_psz_entities_in_silver.sql fails when an entity yields no row)
anchors as (
    select * from (
        values
        ('pensioners_dec', 'Wiek w latach'),
        ('pension_amount_dec', 'Wysokość świadczenia'),
        ('insured_pension', 'Wiek w latach'),
        ('foreign_insured', 'Według stanu na'),
        ('insured_sickness_voiv', 'Wyszczególnienie'),
        ('insured_health', 'Okres'),
        ('payers_voiv', 'Województwo'),
        ('absence_voiv', 'Województwo')
    ) as t (family, header_prefix)
),

header_rows as (
    select
        c.entity,
        min(c.row_number) as header_row
    from cells as c
    inner join anchors as a on c.family = a.family
    where c.column_number = 1 and starts_with(c.text, a.header_prefix)
    group by c.entity
),

headers as (
    select
        c.entity,
        c.column_number,
        lower(c.text) as col_header
    from cells as c
    inner join header_rows as h on c.entity = h.entity and c.row_number = h.header_row
    where c.column_number >= 2
),

labels as (
    select
        c.entity,
        c.row_number,
        c.text as label,
        lower(c.text) as label_l,
        c.cell_type as label_type,
        try_cast(c.value_text as double) as label_number,
        dense_rank() over (partition by c.entity order by c.row_number) as item_order
    from cells as c
    inner join header_rows as h on c.entity = h.entity and c.row_number > h.header_row
    where c.column_number = 1 and c.text <> ''
),

voivodeships as (
    select * from (
        values
        ('dolnośląskie', '02'), ('kujawsko-pomorskie', '04'), ('lubelskie', '06'), ('lubuskie', '08'),
        ('łódzkie', '10'), ('małopolskie', '12'), ('mazowieckie', '14'), ('opolskie', '16'),
        ('podkarpackie', '18'), ('podlaskie', '20'), ('pomorskie', '22'), ('śląskie', '24'),
        ('świętokrzyskie', '26'), ('warmińsko-mazurskie', '28'), ('wielkopolskie', '30'),
        ('zachodniopomorskie', '32')
    ) as t (name_l, area_code)
),

raw as (
    select
        c.entity,
        c.family,
        c.file_year,
        c.file_quarter,
        l.row_number,
        l.label,
        l.label_l,
        l.label_type,
        l.label_number,
        l.item_order,
        h.col_header,
        try_cast(c.value_text as double) as value,
        c.raw_sha256,
        c.source_version,
        c.ingested_at
    from cells as c
    inner join labels as l on c.entity = l.entity and c.row_number = l.row_number
    inner join headers as h on c.entity = h.entity and c.column_number = h.column_number
    where c.column_number >= 2 and c.cell_type = 'n' and try_cast(c.value_text as double) is not null
),

typed as (
    select
        r.*,
        case
            when r.family in ('pensioners_dec', 'pension_amount_dec') then 'A'
            when r.family in ('insured_health', 'foreign_insured') then 'M'
            else 'Q'
        end as frequency,
        case
            when r.family in ('insured_sickness_voiv', 'payers_voiv', 'absence_voiv')
                then
                    case
                        when starts_with(r.label_l, 'ogółem') then '00'
                        when starts_with(r.label_l, 'nieustalone') then '_U'
                        else v.area_code
                    end
            else '00'
        end as area_code,
        case
            when r.family not in ('pensioners_dec', 'pension_amount_dec', 'insured_pension') then 'T'
            when r.col_header like '%ogółem%' then 'T'
            when r.col_header like '%mężczy%' then 'M'
            when r.col_header like '%kobie%' then 'F'
        end as sex_code,
        case
            when r.family in ('pensioners_dec', 'insured_pension')
                then case when starts_with(r.label_l, 'ogółem') then '_T' else r.label end
            when r.family = 'foreign_insured'
                then case when r.col_header like '%ukraiń%' then 'UA' else '_T' end
            when r.family = 'payers_voiv'
                then case
                    when starts_with(r.col_header, 'ogółem') then '_T'
                    when r.col_header like 'płatnik składek - osoba fizyczna płacąca sama%' then 'SELF'
                    when r.col_header like 'zakres liczby ubezpieczonych%'
                        then replace(replace(
                            regexp_extract(r.col_header, '([0-9]+ ?- ?[0-9]+|[0-9]+ i więcej)\s*$', 1), ' i więcej', '+'
                        ), ' ', '')
                end
            else '_T'
        end as breakdown_code,
        case
            when r.family = 'pensioners_dec' then 'pensioners'
            when r.family = 'pension_amount_dec' then 'avg_pension'
            when r.family = 'insured_pension' then 'insured_pension'
            when r.family = 'insured_sickness_voiv' then 'insured_sickness'
            when r.family = 'insured_health' then 'insured_health'
            when r.family = 'foreign_insured' then 'foreign_insured'
            when r.family = 'payers_voiv' then 'payers'
            when r.family = 'absence_voiv' and starts_with(r.col_header, 'liczba dni absencji') then 'sick_leave_days'
            when r.family = 'absence_voiv' and starts_with(r.col_header, 'liczba zaświadczeń')
                then 'sick_leave_certificates'
        end as measure,
        case
            when r.family = 'foreign_insured' and r.label_type = 'n'
                then cast(date '1899-12-30' + cast(round(r.label_number) as integer) as date)
            when r.family = 'foreign_insured' then cast(try_strptime(r.label, '%d.%m.%Y') as date)
        end as stock_date
    from raw as r
    left join voivodeships as v on regexp_replace(r.label_l, '\s*-\s*', '-', 'g') = v.name_l
    where
        -- rows each family models (others: the amount distribution, the average age, powiats)
        (r.family <> 'pension_amount_dec' or starts_with(r.label_l, 'przeciętna wysokość na osobę'))
        and (r.family <> 'pensioners_dec' or not starts_with(r.label_l, 'średni wiek'))
        and (r.family <> 'insured_sickness_voiv' or starts_with(r.label_l, 'ogółem') or v.area_code is not null)
        and (r.family <> 'insured_health' or regexp_extract(r.label, '(20[0-9]{2})', 1) = cast(r.file_year as varchar))
),

periods as (
    select
        *,
        case
            when frequency = 'A' then make_date(file_year, 1, 1)
            when frequency = 'Q' then make_date(file_year, 3 * file_quarter - 2, 1)
            when family = 'insured_health' then make_date(file_year, cast(item_order as integer), 1)
            else make_date(year(stock_date), month(stock_date), 1)
        end as period_start
    from typed
)

select
    entity,
    family,
    frequency,
    period_start,
    case
        when frequency = 'A' then cast(year(period_start) as varchar)
        when frequency = 'Q' then year(period_start) || '-Q' || quarter(period_start)
        else strftime(period_start, '%Y-%m')
    end as period_label,
    area_code,
    case when area_code = '00' then 'Polska' else label end as area_name,
    sex_code,
    nullif(breakdown_code, '') as breakdown_code,
    label as row_label,
    col_header as column_label,
    measure,
    case
        when measure = 'avg_pension' then 'PLN'
        when measure = 'payers' then 'payers'
        when measure = 'sick_leave_days' then 'days'
        when measure = 'sick_leave_certificates' then 'certificates'
        else 'persons'
    end as unit_code,
    value,
    raw_sha256,
    source_version,
    ingested_at
from periods
