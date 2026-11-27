{# Barometr Zawodów (register PLS-030, #454): the forecast category of every occupation the experts assessed, per
   forecast year and area, as published on the edition's posters (bronze.barometr_zawodow_occupations, newest copy of
   each file). Grain: forecast_year x area_code x occupation_name.
   - Poland: the national poster (plakat_<year>); the 16 voivodeships: the poster of each voivodeship page
     (plakat_p<NN>_<year>). One file per area and year; a second file for an area fails the unique test.
   - A line printed twice in the same block of one poster (Bronze repeat_of_item, the same name and category; łódzkie
     2021 "Administratorzy stron internetowych") is the same classification printed again: only the first is kept.
     A name printed in two categories never reaches Silver (Bronze rejects the file).
   - A forecast, not an observation: the edition is the forecast year (survey in Q3/Q4 of the year before); an
     occupation is named as printed on that edition's posters (the Barometr's own list, no code on the posters;
     names and the list change between editions, so there is no cross-year mapping).
   - category_label as printed on the block's tab; category_key is it in lower case without diacritics (deficyt,
     rownowaga, nadwyzka in the editions read; the closed list of the tests comes from the data).
   - area_code: 'PL', or the voivodeship's TERC code from the printed name (macro poc_barometr_zawodow_voivodeships);
     a name not in the list stays NULL and fails the completeness test. #}
with occupations as (
    select *
    from {{ source('barometr_zawodow', 'barometr_zawodow_occupations') }}
    qualify dense_rank() over (partition by entity order by ingested_at desc, source_version desc) = 1
),

picked as (
    select
        *,
        case when entity like 'plakat\_p%' escape '\' then 'province' else 'national' end as file_kind,
        regexp_replace(
            replace(lower(strip_accents(regexp_replace(area_label, '^województwo\s+', ''))), 'ł', 'l'),
            '[^a-z]',
            '',
            'g'
        ) as name_key
    from occupations
    where repeat_of_item is null
),

voivodeships as (
    {{ poc_barometr_zawodow_voivodeships() }}
)

select
    cast(p.edition_year as integer) as forecast_year,
    case when p.area_label = 'Polska' then 'PL' else v.area_code end as area_code,
    p.area_label,
    p.occupation_name,
    p.category_label,
    regexp_replace(replace(lower(strip_accents(p.category_label)), 'ł', 'l'), '[^a-z]+', '_', 'g') as category_key,
    p.poster_categories,
    p.file_kind,
    p.entity,
    p.page_number,
    p.item_number,
    p.source_version,
    p.raw_sha256
from picked as p
left join voivodeships as v on p.name_key = v.name_key
