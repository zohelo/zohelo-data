{# MRPiPS registered unemployment at the end of each month (register PLS-029), from the newest Landing version of each
   month's "statystyki strukturalne" ZIP (bronze.mrpips_unemployment_cells, one row per workbook cell).
   Grain: period_date x area_code x measure_code x sex_code.
   - rate workbook ("Stopa bezrobocia rejestrowanego"): columns A WOJ. and B POW. (TERYT codes; POW. 00 = the
     voivodeship, 0/0 = Poland), C name, D registered unemployed in thousands, E registered unemployment rate in %:
     measures unemployed_thousands and unemployment_rate (sex T), Poland, voivodeships and powiats.
   - MRPiPS-01 TAB 2: end-of-month stock, column B "ogółem bezrobotni", one section per sex (Ogółem T, Kobiety F,
     Mężczyźni M when present) with rows POLSKA and the voivodeships: measure unemployed_persons (persons). The
     voivodeship code comes from the rate workbook's "Woj. <NAME>" row of the same month (by name).
   Values as published (no rescaling); a cell that is not a number is not loaded. #}
with newest as (
    select *
    from {{ source('mrpips_unemployment', 'mrpips_unemployment_cells') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

months as (
    select distinct
        entity,
        make_date(cast(split_part(entity, '_', 2) as integer), cast(split_part(entity, '_', 3) as integer), 1)
            as period_date
    from newest
),

rate_rows as (
    select
        entity,
        sheet_name,
        row_number,
        any_value(source_version) as source_version,
        any_value(raw_sha256) as raw_sha256,
        any_value(ingested_at) as ingested_at,
        max(case when column_number = 1 then trim(value_text) end) as woj,
        max(case when column_number = 2 then trim(value_text) end) as pow,
        max(case when column_number = 3 then trim(value_text) end) as area_name,
        max(case when column_number = 4 then value_text end) as thousands_text,
        max(case when column_number = 5 then value_text end) as rate_text
    from newest
    where workbook = 'rate'
    group by entity, sheet_name, row_number
),

rate_areas as (
    select
        *,
        lpad(woj, 2, '0') || lpad(pow, 2, '0') as area_code,
        case
            when cast(woj as integer) = 0 then 'country' when cast(pow as integer) = 0 then 'voivodeship'
            else 'powiat'
        end as area_level,
        upper(trim(regexp_replace(area_name, '^(Woj\.|woj\.)\s*', ''))) as name_key
    from rate_rows
    where regexp_full_match(woj, '[0-9]{1,2}') and regexp_full_match(pow, '[0-9]{1,2}')
),

rate_values as (
    select
        entity,
        area_code,
        area_level,
        area_name,
        'unemployed_thousands' as measure_code,
        'T' as sex_code,
        try_cast(thousands_text as double) as value,
        'thousand_persons' as unit_code,
        source_version,
        raw_sha256,
        ingested_at
    from rate_areas
    union all
    select
        entity,
        area_code,
        area_level,
        area_name,
        'unemployment_rate' as measure_code,
        'T' as sex_code,
        try_cast(rate_text as double) as value,
        'pct' as unit_code,
        source_version,
        raw_sha256,
        ingested_at
    from rate_areas
),

report_rows as (
    select
        entity,
        row_number,
        any_value(source_version) as source_version,
        any_value(raw_sha256) as raw_sha256,
        any_value(ingested_at) as ingested_at,
        max(case when column_number = 1 then trim(value_text) end) as label,
        max(case when column_number = 2 then trim(value_text) end) as col_b
    from newest
    where workbook = 'report' and sheet_name = 'TAB 2'
    group by entity, row_number
),

report_sections as (
    select
        *,
        -- the section (sex) is set by the nearest row above whose column B is a section title
        last_value(
            case lower(col_b) when 'ogółem' then 'T' when 'kobiety' then 'F' when 'mężczyźni' then 'M' end
            ignore nulls
        ) over (partition by entity order by row_number rows between unbounded preceding and current row) as sex_code
    from report_rows
),

report_values as (
    select
        s.entity,
        coalesce(r.area_code, case when upper(s.label) = 'POLSKA' then '0000' end) as area_code,
        case when upper(s.label) = 'POLSKA' then 'country' else 'voivodeship' end as area_level,
        s.label as area_name,
        'unemployed_persons' as measure_code,
        s.sex_code,
        try_cast(s.col_b as double) as value,
        'persons' as unit_code,
        s.source_version,
        s.raw_sha256,
        s.ingested_at
    from report_sections as s
    left join rate_areas as r
        on s.entity = r.entity and r.area_level = 'voivodeship' and upper(s.label) = r.name_key
    where
        s.sex_code is not null
        and try_cast(s.col_b as double) is not null
        and (upper(s.label) = 'POLSKA' or r.area_code is not null)
)

select
    m.period_date,
    v.entity,
    v.area_code,
    v.area_level,
    v.area_name,
    v.measure_code,
    v.sex_code,
    v.value,
    v.unit_code,
    v.source_version,
    v.raw_sha256,
    v.ingested_at
from (
    select * from rate_values
    union all
    select * from report_values
) as v
inner join months as m on v.entity = m.entity
where v.value is not null
