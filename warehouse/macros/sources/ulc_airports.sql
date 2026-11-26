{# ulc_airports (register PL-ENV-026): the airports of the ULC tables and the Gold registration, decision D34
   (docs/source-registration.md). The tables name airports only (no ICAO or IATA code), in changing spellings
   ("1. Warszawa im. Chopina", "Lotnisko Chopina w Warszawie"); poc_ulc_airports_list() is the correspondence of this
   build: one row per airport, its ICAO and IATA codes, the TERYT voivodeship it lies in and a pattern that matches
   every printed name of it (lower case) and no other row's names. '_T' = the tables' total row "Suma" (all listed
   airports, Poland). The Silver model fails (not_null airport_code) on a printed name that matches none or several. #}
{% macro poc_ulc_airports_list() %}
    select * from (values
        ('EPWA', 'WAW', 'Warszawa-Chopin', '14', 'chopin'),
        ('EPKK', 'KRK', 'Kraków-Balice', '12', 'krak[oó]w'),
        ('EPGD', 'GDN', 'Gdańsk im. Lecha Wałęsy', '22', 'gdańsk'),
        ('EPKT', 'KTW', 'Katowice-Pyrzowice', '24', 'katowice'),
        ('EPWR', 'WRO', 'Wrocław-Strachowice', '02', 'wrocław'),
        ('EPPO', 'POZ', 'Poznań-Ławica', '30', 'poznań'),
        ('EPMO', 'WMI', 'Warszawa/Modlin', '14', 'modlin'),
        ('EPRZ', 'RZE', 'Rzeszów-Jasionka', '18', 'rzeszów'),
        ('EPLL', 'LCJ', 'Łódź-Lublinek', '10', 'łódź'),
        ('EPBY', 'BZG', 'Bydgoszcz-Szwederowo', '04', 'bydgoszcz'),
        ('EPSC', 'SZZ', 'Szczecin-Goleniów', '32', 'szczecin'),
        ('EPLB', 'LUZ', 'Lublin', '06', '(^|[^a-z])lublin($|[^a-z])'),
        ('EPSY', 'SZY', 'Olsztyn-Mazury', '28', 'olsztyn'),
        ('EPZG', 'IEG', 'Zielona Góra-Babimost', '08', 'zielona góra'),
        ('EPRA', 'RDO', 'Warszawa-Radom', '14', 'radom'),
        ('_T', null, 'Suma (wszystkie porty w tabeli)', null, '^suma$')
    ) as t (airport_code, iata_code, airport_name, terc_woj_code, name_pattern)
{% endmacro %}

{% macro poc_source__ulc_airports() %}
    {{ return({'block_model': 'poc_build_ulc_airports', 'blocks': [['airport_traffic', 'ULC airport traffic: passengers and passenger flight operations (quarterly)']]}) }}
{% endmacro %}

{# Geography: an airport → its voivodeship (TERC), the total row → Poland. The airport itself is the series
   breakdown (poc_build_ulc_airports). Static list, so the shared map never waits for this source. #}
{% macro poc_geography__ulc_airports() %}
ulc_airports as (
    select
        'ulc_airports' as source_system,
        a.airport_code as source_code,
        case when a.airport_code = '_T' then 'PL' else coalesce(g.geography_key, '_X') end as geography_key
    from ({{ poc_ulc_airports_list() }}) as a
    left join geo as g on g.level_name = 'wojewodztwo' and g.terc_code = a.terc_woj_code
)
{% endmacro %}
