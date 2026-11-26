{# Barometr Zawodów (register PLS-030): every edition has Poland and the 16 voivodeships, every printed voivodeship
   name maps to a code, every category label is known, and every occupation of a voivodeship poster is on the Poland
   poster of the same edition (the national list is the full list of assessed occupations; a name that is not on it
   is a line the poster reader joined or split wrongly). Returns one row per problem. #}
with f as (
    select * from {{ ref('barometr_zawodow_forecasts') }}
),

areas as (
    select
        forecast_year,
        'areas: ' || count(distinct area_code) || ' of 17' as problem
    from f
    group by forecast_year
    having count(distinct area_code) <> 17 or count(distinct area_code) filter (where area_code = 'PL') <> 1
),

unmapped as (
    select
        forecast_year,
        'area name without a code: ' || area_label as problem
    from f
    where area_code is null
    group by all
),

categories as (
    select
        forecast_year,
        'category label not known: ' || category_label as problem
    from f
    where category_key not in ('deficyt', 'rownowaga', 'nadwyzka') or category_key is null
    group by all
),

off_list as (
    select
        v.forecast_year,
        'not on the Poland poster: ' || v.area_code || ' ' || v.occupation_name as problem
    from f as v
    where
        v.area_code <> 'PL'
        and not exists (
            select 1
            from f as p
            where p.area_code = 'PL' and p.forecast_year = v.forecast_year and p.occupation_name = v.occupation_name
        )
)

select * from areas
union all
select * from unmapped
union all
select * from categories
union all
select * from off_list
