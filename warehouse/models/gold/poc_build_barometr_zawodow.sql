{{ config(alias='build_barometr_zawodow', tags=['gold_block']) }}
{# Gold build stage: Barometr Zawodów (register PLS-030, #454), block 'forecast', from
   silver.barometr_zawodow_forecasts.
   A forecast is not an observation: this block holds no forecast value. It counts the published classifications:
   how many occupations an edition's poster puts in each forecast category, per area (Poland and the 16
   voivodeships) and forecast year. measure_key barometr_zawodow:forecast:occupations:<category_key> (deficyt,
   rownowaga, nadwyzka), unit 'occupations', annual, date_key = 1 January of the forecast year (the year the
   forecast is for; the survey ran in the autumn before). Every category an edition uses (any area) gets a row for
   every area of that edition, 0 when the area's poster has no occupation in it: each poster lists every occupation
   assessed for its area, so 0 is a count, not a missing value. Every row has observation status 'forecast' (as
   UN WPP projections and Eurostat PROJ_* rows, docs/gold-coverage.md). Never merged with vacancy or unemployment
   counts; the occupation-level table stays in Silver. #}
with f as (
    select * from {{ ref('barometr_zawodow_forecasts') }}
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'barometr_zawodow'
),

editions as (  -- one file per area and year (Silver: the national poster and the 16 voivodeship posters); a
    -- second file would repeat the area's rows and fail the grain checks, never be picked
    select distinct
        forecast_year,
        area_code,
        source_version,
        raw_sha256
    from f
),

categories as (  -- the categories an edition uses, in any area
    select distinct
        forecast_year,
        category_key
    from f
),

grid as (
    select
        e.forecast_year,
        e.area_code,
        c.category_key,
        e.source_version,
        e.raw_sha256
    from editions as e
    inner join categories as c on e.forecast_year = c.forecast_year
),

counts as (
    select
        forecast_year,
        area_code,
        category_key,
        count(*) as occupations
    from f
    group by forecast_year, area_code, category_key
)

select
    'barometr_zawodow' as source_system,
    'forecast' as block_id,
    'Barometr Zawodów: occupations by forecast category (counts of published classifications)' as block_name,
    'barometr_zawodow:forecast:occupations:' || g.category_key as measure_key,
    case g.category_key
        when 'deficyt' then 'Occupations forecast in deficit (Barometr Zawodów, count of published classifications)'
        when 'rownowaga' then 'Occupations forecast in balance (Barometr Zawodów, count of published classifications)'
        when 'nadwyzka' then 'Occupations forecast in surplus (Barometr Zawodów, count of published classifications)'
    end as measure_name,
    g.category_key as measure_codes,
    'occupations' as unit_code,
    'occupations' as unit_name,
    g.area_code as geography_source_code,
    coalesce(geo.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(g.forecast_year, 1, 1) as date_key,
    cast(g.forecast_year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(coalesce(c.occupations, 0) as double) as value,
    'forecast' as observation_status_key,  -- counts of a forecast: never presented as an observation
    g.source_version,
    g.raw_sha256
from grid as g
left join counts as c
    on g.forecast_year = c.forecast_year and g.area_code = c.area_code and g.category_key = c.category_key
left join geo on g.area_code = geo.source_code
