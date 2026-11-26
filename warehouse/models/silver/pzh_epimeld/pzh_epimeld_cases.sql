{# NIZP PZH-PIB EPIMELD (register PLS-019): registered cases of infectious diseases and poisonings and incidence, per
   year, disease and area (Poland, voivodeships), from the per-disease voivodeship tables of the annual bulletins
   "Choroby zakaźne i zatrucia w Polsce" (bronze.pzh_epimeld_rows, newest Landing copy of each bulletin).
   Grain: period_year x disease_key x area_code.
   - disease_key: the disease line as published (name and ICD-10 codes), without spaces, accents and punctuation, so
     that the text layer's split letters ("ZESP ÓŁ") do not split a series; plus ':reported' or ':diagnosed' for the
     tables of reported or diagnosed cases (AIDS prints both). The ICD codes alone do not identify a disease (the
     "U DZIECI DO LAT 2" tables, CJD and vCJD, the influenza tables share codes). A disease whose name or codes change
     between bulletins is a new series.
   - Each bulletin prints its year and, for Poland, the year before. Rule (a version choice, never a pick by row
     order): a value comes from the newest bulletin that prints its year x disease x area (rank over edition_year),
     so Poland's year Y comes from bulletin Y+1 when it exists (it carries the corrections reported after bulletin
     Y); voivodeships are printed for the bulletin's own year only. editions_printing counts the bulletins that print
     the key. Two rows of one bulletin for one key tie on the rank and fail the unique test.
   - "-" (no case registered), "x" and "." are not values: the chosen row is left out when its cases are not a number,
     never read as 0; a row Bronze could not read (parse_note) is a chosen row without a value, so it is left out too
     and an older bulletin's value is not used instead. Incidence is null when not a number. Quarters and
     hospitalisation are not loaded.
   - area_code: 'PL' or the TERYT voivodeship code (the bulletins number voivodeships 1-16 in alphabetical order,
     which is TERYT code / 2).
   - incidence_header: the denominator as printed ("na 100 tys.", or "na 100 tys. urodzeń żywych" for the congenital
     diseases). incidence_unit_code keeps the denominator with the measure: per 100,000 live births; per 100,000
     children under 2 or aged 0-14 for the tables "U DZIECI DO LAT 2" and "U DZIECI /0-14 LAT/" (their header says only
     "na 100 tys."; cases / incidence gives about 0.5-0.8 million and 5.8 million, the children of those ages, against
     about 38 million for the other tables); else per 100,000 population. A block whose POLSKA rows give cases /
     incidence x 100,000 (within the incidence's printed rounding) outside the band of that denominator (population 30-46 million, live
     births 0.2-0.6 million, children under 2 0.3-1.2 million, aged 0-14 4-8 million) has its incidence dropped
     (incidence_dropped; the bulletins 2013 and 2014 print the A04 children-under-2 table per 100,000 population, 5.5
     instead of about 260; the congenital syphilis tables 2013-2025 and the puerperal fever table B95.0/O85 give a
     denominator of births under the header "na 100 tys."). preliminary: the bulletin's text says "wstępne".
   - poland_cases_in_edition, table_read, block_quarters_match: the POLSKA cases of the same block and bulletin,
     whether every row of that block was read, and whether every row's printed quarters add up to its printed year
     (quarters_match; false where the bulletin misprints a total, e.g. Wielkopolskie in the A41.3 tables 2020-2025),
     for the test that the voivodeships add up to Poland. #}
with newest as (
    select *
    from {{ source('pzh_epimeld', 'pzh_epimeld_rows') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

printed as (
    select
        *,
        trim(
            regexp_replace(
                lower(strip_accents(
                    regexp_replace(replace(replace(disease_label, 'ł', 'l'), 'Ł', 'L'), '\s+', '', 'g')
                )),
                '[^a-z0-9]+', '_', 'g'
            ),
            '_'
        ) || case
            when table_title ilike 'Liczba rozpoznanych%' then ':diagnosed'
            when table_title ilike 'Liczba zg%oszonych%' then ':reported'
            else ''
        end as disease_key,
        case
            when row_kind = 'poland' then 'PL'
            else lpad(cast(area_number * 2 as varchar), 2, '0')
        end as area_code,
        max(case when row_kind = 'poland' and period_year = edition_year then value_cases end) over (
            partition by entity, table_number
        ) as poland_cases_text,
        case
            when incidence_header = 'na 100 tys. urodzeń żywych' then 'per_100k_live_births'
            when incidence_header <> 'na 100 tys.' then null
            when disease_label ilike '%U DZIECI DO LAT 2%' then 'per_100k_children_under_2'
            when disease_label ilike '%U DZIECI /0-14 LAT/%' then 'per_100k_children_0_14'
            else 'per_100k'
        end as incidence_unit_code,
        count(parse_note) over (partition by entity, table_number) = 0 as table_read,
        -- the printed quarters add up to the printed year (a check only: "-" counts as no case here)
        coalesce(try_cast(value_q1 as bigint), 0) + coalesce(try_cast(value_q2 as bigint), 0)
        + coalesce(try_cast(value_q3 as bigint), 0) + coalesce(try_cast(value_q4 as bigint), 0)
        = try_cast(value_cases as bigint) as quarters_match
    from newest
    -- a voivodeship block Bronze could not read has no rows to choose; a row printed with a year that is not the
    -- bulletin's or the one before (a misprint) does not stand for that year
    where row_kind in ('poland', 'voivodeship') and coalesce(parse_note, '') not like 'year %'
),

blocks as (
    select
        *,
        bool_and(coalesce(quarters_match, true)) over (partition by entity, table_number) as block_quarters_match,
        -- the denominator check: every POLSKA row of the block with a case gives a denominator, cases / incidence x
        -- 100,000 with the incidence's printed rounding (half a unit of its last decimal), that can lie in the band of
        -- the unit's denominator (Poland 2005-2025)
        coalesce(bool_and(
            p_cases * 1e5 / (p_inc + p_half) <= case incidence_unit_code
                when 'per_100k' then 46e6 when 'per_100k_live_births' then 6e5
                when 'per_100k_children_under_2' then 12e5 else 8e6
            end
            and (p_inc <= p_half or p_cases * 1e5 / (p_inc - p_half) >= case incidence_unit_code
                when 'per_100k' then 30e6 when 'per_100k_live_births' then 2e5
                when 'per_100k_children_under_2' then 3e5 else 4e6
            end)
        ) filter (where row_kind = 'poland' and p_cases > 0 and p_inc > 0) over (
            partition by entity, table_number
        ), true) as denominator_ok
    from (
        select
            *,
            try_cast(value_cases as bigint) as p_cases,
            try_cast(replace(value_incidence, ',', '.') as double) as p_inc,
            0.5 / 10 ^ length(split_part(value_incidence, ',', 2)) as p_half
        from printed
    ) as p
),

chosen as (
    select
        *,
        count(*) over (partition by disease_key, area_code, period_year) as editions_printing
    from blocks
    qualify rank() over (partition by disease_key, area_code, period_year order by edition_year desc) = 1
)

select
    period_year,
    make_date(cast(period_year as integer), 1, 1) as period_date,
    disease_key,
    disease_label,
    upper(regexp_replace(disease_codes, '\s+', '', 'g')) as disease_codes,
    area_code,
    area_number,
    area_label,
    cast(value_cases as bigint) as cases,
    case
        when denominator_ok and regexp_full_match(value_incidence, '\d+(,\d+)?')
            then cast(replace(value_incidence, ',', '.') as double)
    end as incidence,
    incidence_header,
    incidence_unit_code,
    not denominator_ok as incidence_dropped,
    case when regexp_full_match(poland_cases_text, '\d+') then cast(poland_cases_text as bigint) end
        as poland_cases_in_edition,
    table_read,
    quarters_match,
    block_quarters_match,
    table_note,
    row_marks,
    preliminary = 'true' as preliminary,
    edition_year as source_edition,
    editions_printing,
    entity as source_entity,
    source_version,
    raw_sha256,
    ingested_at
from chosen
where parse_note is null and regexp_full_match(value_cases, '\d+')
