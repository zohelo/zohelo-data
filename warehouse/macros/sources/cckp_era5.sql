{# cckp_era5 (register GL-036, #2421): Gold registration, decision D34 (docs/source-registration.md). Block 'era5'
   (models/gold/poc_build_cckp_era5.sql). #}
{% macro poc_source__cckp_era5() %}
    {{ return({'block_model': 'poc_build_cckp_era5', 'blocks': [['era5', 'World Bank CCKP: ERA5 0.25-degree air temperature and precipitation']]}) }}
{% endmacro %}

{# Stated code map of Poland's first-level units in CCKP: CCKP code, the name CCKP publishes in geonames.json (no
   diacritics, its own spellings 'Lubeiskie' and 'Malopolske'; read in Dev probe 45810479823 and trial 45826698285)
   and the TERC voivodeship code. Nothing is matched by name or position. The extractor requests exactly these codes
   (POL_ADM1) and fails when an answer lacks one; the test cckp_era5_observations_complete fails on a first-level code
   outside this list or a listed code without data. #}
{% macro poc_cckp_era5_voivodeships() %}
    select * from (
        values
        ('POL.1982370', 'Dolnoslaskie', '02'), ('POL.1982371', 'Kujawsko-Pomorskie', '04'),
        ('POL.1982372', 'Lodzkie', '10'), ('POL.1982373', 'Lubeiskie', '06'), ('POL.1982374', 'Lubuskie', '08'),
        ('POL.1982375', 'Malopolske', '12'), ('POL.1982376', 'Mazowieckie', '14'), ('POL.1982377', 'Opolskie', '16'),
        ('POL.1982378', 'Podkarpackie', '18'), ('POL.1982379', 'Podlaskie', '20'), ('POL.1982380', 'Pomorskie', '22'),
        ('POL.1982381', 'Slaskie', '24'), ('POL.1982382', 'Swietokrzyskie', '26'),
        ('POL.1982383', 'Warminsko-Mazurskie', '28'), ('POL.1982384', 'Wielkopolskie', '30'),
        ('POL.1982385', 'Zachodnio-Pomorskie', '32')
    ) as t (geocode, cckp_name, terc_woj)
{% endmacro %}

{# Countries by ISO3 (dim_geography iso3, the World Bank economy code, as unhcr), except Kosovo, which CCKP codes KSV
   (geonames.json 'Kosovo') and the World Bank XKX (as owid); voivodeships by the stated map above. Static, so the map
   does not read CCKP Silver. A CCKP code without a Gold country (29 territories and Taiwan in Dev trial 45831607817,
   e.g. GUF, REU, JEY, TWN) maps to _X with its code kept and is counted by build_check. #}
{% macro poc_geography__cckp_era5() %}
cckp_era5 as (
    select
        'cckp_era5' as source_system,
        g.iso3 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 is not null
    union all
    select
        'cckp_era5' as source_system,
        'KSV' as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso3 = 'XKX'
    union all
    select
        'cckp_era5' as source_system,
        v.geocode as source_code,
        g.geography_key
    from ({{ poc_cckp_era5_voivodeships() }}) as v
    inner join geo as g on g.level_name = 'wojewodztwo' and left(g.terc_code, 2) = v.terc_woj
)
{% endmacro %}
