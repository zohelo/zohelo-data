{# internet_gov_broadband (register PL-ENV-034, #2963): Gold registration, decision D34 (docs/source-registration.md).
   Block 'coverage' (models/gold/poc_build_internet_gov_broadband.sql). The report names voivodeships, not codes:
   Silver maps the 16 printed names to their TERC codes through the static list below (complete; the Silver test
   tests/internet_gov_broadband_coverage_complete.sql fails when a name is not in it or a voivodeship is missing),
   so the geography map reads codes, never names. #}
{% macro poc_internet_gov_broadband_voivodeships() %}
    select * from (
        values
        ('dolnośląskie', '02'),
        ('kujawsko-pomorskie', '04'),
        ('lubelskie', '06'),
        ('lubuskie', '08'),
        ('łódzkie', '10'),
        ('małopolskie', '12'),
        ('mazowieckie', '14'),
        ('opolskie', '16'),
        ('podkarpackie', '18'),
        ('podlaskie', '20'),
        ('pomorskie', '22'),
        ('śląskie', '24'),
        ('świętokrzyskie', '26'),
        ('warmińsko-mazurskie', '28'),
        ('wielkopolskie', '30'),
        ('zachodniopomorskie', '32')
    ) as t (area_name, area_code)
{% endmacro %}

{% macro poc_source__internet_gov_broadband() %}
    {{ return({'block_model': 'poc_build_internet_gov_broadband', 'blocks': [['coverage', 'internet.gov.pl fixed broadband coverage of address points (quarterly)']]}) }}
{% endmacro %}

{% macro poc_geography__internet_gov_broadband() %}
internet_gov_broadband as (
    select
        'internet_gov_broadband' as source_system,
        v.area_code as source_code,
        'TERC:' || v.area_code as geography_key
    from ({{ poc_internet_gov_broadband_voivodeships() }}) as v
    union all
    select
        'internet_gov_broadband' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
