{# Barometr Zawodów (register PLS-030): every edition has Poland and the 16 voivodeships, every printed voivodeship
   name maps to a code, every category label is known, and every poster lists at least 100 occupations (the posters
   read hold 122-173; fewer means a block was not read). Voivodeship names are not checked against the Poland
   poster: the 2016 national poster lists only occupations assessed in at least half of the powiats. Returns one row
   per problem. #}
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

short as (
    select
        forecast_year,
        'only ' || count(*) || ' occupations on the poster of ' || area_code as problem
    from f
    group by forecast_year, area_code
    having count(*) < 100
)

select * from areas
union all
select * from unmapped
union all
select * from categories
union all
select * from short
