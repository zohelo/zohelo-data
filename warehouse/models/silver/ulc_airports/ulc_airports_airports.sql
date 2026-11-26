{# ULC airports (register PL-ENV-026): the airports of the ULC quarterly tables as an entity table.
   Grain: airport_code (ICAO). Codes, voivodeship and name patterns from macro poc_ulc_airports_list (the tables
   print names only); the names as printed, the first and last quarter with a value and the newest quarter with
   passengers from ulc_airports_traffic. Airports of the list that no table prints are kept (first_period null). #}
with traffic as (
    select
        airport_code,
        min(period_date) as first_period,
        max(period_date) as last_period,
        max(period_date) filter (where measure_code = 'passengers' and value > 0) as last_period_with_passengers,
        string_agg(distinct airport_label, ' | ' order by airport_label) as names_published
    from {{ ref('ulc_airports_traffic') }}
    group by airport_code
)

select
    a.airport_code,
    a.iata_code,
    a.airport_name,
    a.terc_woj_code,
    tr.names_published,
    tr.first_period,
    tr.last_period,
    tr.last_period_with_passengers
from ({{ poc_ulc_airports_list() }}) as a
left join traffic as tr on a.airport_code = tr.airport_code
where a.airport_code <> '_T'
