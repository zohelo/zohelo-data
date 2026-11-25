{# ULC airport traffic (register PL-ENV-026): passengers handled and passenger flight operations (take-offs and
   landings) in regular and charter traffic per Polish airport and in total, per quarter, from the quarter tables of
   the quarterly PDFs (bronze.ulc_airports_rows, newest Landing copy of each quarter's file).
   Grain: period_date x airport_code x measure_code.
   - Each quarter's table prints the quarter and the same quarter of the two previous years (columns 1-3; the growth
     columns 4-5 are derived and not loaded), so a quarter appears in up to three files: the value is taken from the
     newest file that prints it (files ordered by their own quarter, the entity), which carries later corrections
     (e.g. Wrocław Q3 2019: 1,120,216 in the Q3 2019 file, 1,135,810 in the Q3 2020 and Q3 2021 files).
   - Q1 files print the quarter table twice: both copies are kept as rows of that file and must agree
     (value_variants = 1, tested).
   - The cumulative tables (first half, three quarters, whole year) are not loaded.
   - "-" and "b/d" mean no value: such a cell is not loaded (never 0). A printed 0 is kept (airports without traffic:
     before opening, Radom while closed 2018-2023, Q2 2020).
   - airport_code: ICAO code by the printed name (macro poc_ulc_airports_list), '_T' = the total row "Suma". #}
with newest as (
    select *
    from {{ source('ulc_airports', 'ulc_airports_rows') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

cells as (
    select
        b.entity,
        b.table_number,
        b.airport_label,
        case
            when b.measure_label ilike '%operacj%' then 'passenger_operations'
            when b.measure_label ilike '%pasażer%' then 'passengers'
        end as measure_code,
        cast(split_part(b.column_labels, '|', k.col) as integer) as period_year,
        case b.period_header
            when 'pierwszy kwartał' then 1 when 'drugi kwartał' then 2
            when 'trzeci kwartał' then 3 when 'czwarty kwartał' then 4
        end as period_quarter,
        [b.value_1, b.value_2, b.value_3][k.col] as value_text,
        b.source_version,
        b.raw_sha256,
        b.ingested_at
    from newest as b
    cross join (values (1), (2), (3)) as k (col)
    where b.period_header in ('pierwszy kwartał', 'drugi kwartał', 'trzeci kwartał', 'czwarty kwartał')
),

printed as (
    select distinct airport_label from cells
),

airport_list as ({{ poc_ulc_airports_list() }}),

labels as (
    -- each printed name to the one airport whose pattern matches it (null when none or several match)
    select
        n.airport_label,
        case when count(a.airport_code) = 1 then max(a.airport_code) end as airport_code
    from printed as n
    left join airport_list as a
        on regexp_matches(lower(regexp_replace(n.airport_label, '^[0-9]+\.\s*', '')), a.name_pattern)
    group by n.airport_label
),

latest as (
    -- the newest file that prints the quarter (a version choice by the file's own quarter)
    select
        c.*,
        l.airport_code
    from cells as c
    left join labels as l on c.airport_label = l.airport_label
    qualify rank() over (
        partition by l.airport_code, c.measure_code, c.period_year, c.period_quarter order by c.entity desc
    ) = 1
)

select
    make_date(period_year, 3 * period_quarter - 2, 1) as period_date,
    period_year,
    period_quarter,
    airport_code,
    string_agg(distinct airport_label, ' | ' order by airport_label) as airport_label,
    measure_code,
    max(try_cast(replace(value_text, ' ', '') as bigint)) as value,
    count(distinct value_text) as value_variants,
    case measure_code when 'passengers' then 'passengers' else 'operations' end as unit_code,
    entity as source_entity,
    source_version,
    raw_sha256,
    ingested_at
from latest
where value_text not in ('-', 'b/d')
group by all
