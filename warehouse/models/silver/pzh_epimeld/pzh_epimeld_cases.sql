{# NIZP PZH-PIB EPIMELD (register PLS-019): registered cases of infectious diseases and poisonings and incidence, per
   year, disease and area (Poland, voivodeships), from the per-disease voivodeship tables of the annual bulletins
   "Choroby zakaźne i zatrucia w Polsce" (bronze.pzh_epimeld_rows, newest Landing copy of each bulletin).
   Grain: period_year x disease_codes x area_code.
   - disease_codes: the ICD-10 codes of the table title as published, spaces removed (the disease key; a title whose
     codes change between bulletins is a new disease series). disease_label: the title in the chosen bulletin.
   - Each bulletin prints its year and, for Poland, the year before. Rule (a version choice, never a pick by row
     order): a value comes from the newest bulletin that prints its year x disease x area (rank over edition_year),
     so Poland's year Y comes from bulletin Y+1 when it exists (it carries the corrections reported after bulletin
     Y); voivodeships are printed for the bulletin's own year only. editions_printing counts the bulletins that print
     the key. Two rows of one bulletin for one key tie on the rank and fail the unique test.
   - "-" (no case registered) and "x" are not values: the chosen row is left out when its cases are not a number,
     never read as 0; incidence is null when not a number. Quarters and hospitalisation are not loaded.
   - area_code: 'PL' or the TERYT voivodeship code (the bulletins number voivodeships 1-16 in alphabetical order,
     which is TERYT code / 2).
   - incidence_header: the denominator as printed ("na 100 tys." = per 100,000 population); preliminary: the
     bulletin's text says "wstępne".
   - poland_cases_in_edition: the POLSKA cases of the same table and bulletin (voivodeship rows), for the test that
     the voivodeships add up to Poland. #}
with newest as (
    select *
    from {{ source('pzh_epimeld', 'pzh_epimeld_rows') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

printed as (
    select
        *,
        upper(regexp_replace(disease_codes, '\s+', '', 'g')) as disease_key,
        case
            when row_kind = 'poland' then 'PL'
            else lpad(cast(area_number * 2 as varchar), 2, '0')
        end as area_code,
        max(case when row_kind = 'poland' and period_year = edition_year then value_cases end) over (
            partition by entity, table_number
        ) as poland_cases_text
    from newest
),

chosen as (
    select
        *,
        count(*) over (partition by disease_key, area_code, period_year) as editions_printing
    from printed
    qualify rank() over (partition by disease_key, area_code, period_year order by edition_year desc) = 1
)

select
    period_year,
    make_date(cast(period_year as integer), 1, 1) as period_date,
    disease_key as disease_codes,
    disease_label,
    area_code,
    area_number,
    area_label,
    cast(value_cases as bigint) as cases,
    case
        when regexp_full_match(value_incidence, '\d+(,\d+)?') then cast(replace(value_incidence, ',', '.') as double)
    end as incidence,
    incidence_header,
    case when incidence_header in ('na 100 tys.', 'na 100 tys') then 'per_100k' end as incidence_unit_code,
    case when regexp_full_match(poland_cases_text, '\d+') then cast(poland_cases_text as bigint) end
        as poland_cases_in_edition,
    preliminary = 'true' as preliminary,
    edition_year as source_edition,
    editions_printing,
    entity as source_entity,
    source_version,
    raw_sha256,
    ingested_at
from chosen
where regexp_full_match(value_cases, '\d+')
