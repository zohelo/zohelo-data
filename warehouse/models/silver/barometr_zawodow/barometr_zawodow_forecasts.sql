{# Barometr Zawodów (register PLS-030, #454): the forecast category of every occupation the experts assessed, per
   forecast year and area, as published on the edition's posters (bronze.barometr_zawodow_occupations, newest copy of
   each file). Grain: forecast_year x area_code x occupation_name.
   - Poland: the national poster file (plakat_<year>); the 16 voivodeships: the posters printed in the national
     report (raport_<year>). The report's own Polska poster (most years) is not used: one source per area, by file
     kind, never a pick between two rows of one key.
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
        split_part(entity, '_', 1) as file_kind,
        regexp_replace(
            replace(lower(strip_accents(regexp_replace(area_label, '^województwo\s+', ''))), 'ł', 'l'), '[^a-z]', '', 'g'
        ) as name_key
    from occupations
    where
        (split_part(entity, '_', 1) = 'plakat' and area_label = 'Polska')
        or (split_part(entity, '_', 1) = 'raport' and area_label <> 'Polska')
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
