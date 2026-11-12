{# gios_air_quality (register PL-ENV-010): Gold registration, decision D34 (docs/source-registration.md). Block
   'air_daily' (models/gold/poc_build_gios_air_quality.sql): daily means of five pollutants for Poland and the
   voivodeships. Areas: the station's voivodeship as the API prints it (upper case), mapped by a static list to the
   two-digit TERC code of dim_geography, and 'POLSKA' = Poland. A name outside the list maps to _X (build_check
   rows_geography_unmatched) and fails the accepted_values test of silver.gios_air_stations first. #}
{% macro poc_gios_air_quality_voivodeships() %}
    select * from (
        values
        ('DOLNOŚLĄSKIE', '02'), ('KUJAWSKO-POMORSKIE', '04'), ('LUBELSKIE', '06'), ('LUBUSKIE', '08'),
        ('ŁÓDZKIE', '10'), ('MAŁOPOLSKIE', '12'), ('MAZOWIECKIE', '14'), ('OPOLSKIE', '16'), ('PODKARPACKIE', '18'),
        ('PODLASKIE', '20'), ('POMORSKIE', '22'), ('ŚLĄSKIE', '24'), ('ŚWIĘTOKRZYSKIE', '26'),
        ('WARMIŃSKO-MAZURSKIE', '28'), ('WIELKOPOLSKIE', '30'), ('ZACHODNIOPOMORSKIE', '32')
    ) as t (voivodeship, terc_code)
{% endmacro %}

{% macro poc_source__gios_air_quality() %}
    {{ return({'block_model': 'poc_build_gios_air_quality', 'blocks': [['air_daily', 'GIOŚ air quality, daily means of hourly values (provisional)']]}) }}
{% endmacro %}

{% macro poc_geography__gios_air_quality() %}
gios_air_quality as (
    select
        'gios_air_quality' as source_system,
        v.voivodeship as source_code,
        g.geography_key
    from ({{ poc_gios_air_quality_voivodeships() }}) as v
    inner join geo as g on v.terc_code = g.terc_code and g.level_name = 'wojewodztwo'
    union all
    select
        'gios_air_quality' as source_system,
        'POLSKA' as source_code,
        'PL' as geography_key
)
{% endmacro %}
