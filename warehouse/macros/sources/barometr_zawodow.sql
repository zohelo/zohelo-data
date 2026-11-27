{# barometr_zawodow (register PLS-030, #454): Gold registration, decision D34 (docs/source-registration.md). Block
   'forecast' (models/gold/poc_build_barometr_zawodow.sql). The posters name voivodeships, not codes ("województwo
   małopolskie", in 2022 without diacritics: "województwo slaskie"): Silver maps the printed name, lower case,
   diacritics and every non-letter removed, to its TERC code through the static list below (complete; the Silver test
   tests/barometr_zawodow_forecasts_complete.sql fails when a name is not in it or an edition misses an area), so the
   geography map reads codes, never names. #}
{% macro poc_barometr_zawodow_voivodeships() %}
    select * from (
        values
        ('dolnoslaskie', '02'),
        ('kujawskopomorskie', '04'),
        ('lubelskie', '06'),
        ('lubuskie', '08'),
        ('lodzkie', '10'),
        ('malopolskie', '12'),
        ('mazowieckie', '14'),
        ('opolskie', '16'),
        ('podkarpackie', '18'),
        ('podlaskie', '20'),
        ('pomorskie', '22'),
        ('slaskie', '24'),
        ('swietokrzyskie', '26'),
        ('warminskomazurskie', '28'),
        ('wielkopolskie', '30'),
        ('zachodniopomorskie', '32')
    ) as t (name_key, area_code)
{% endmacro %}

{% macro poc_source__barometr_zawodow() %}
    {{ return({'block_model': 'poc_build_barometr_zawodow', 'blocks': [['forecast', 'Barometr Zawodów: occupations by forecast category (counts of published classifications)']]}) }}
{% endmacro %}

{% macro poc_geography__barometr_zawodow() %}
barometr_zawodow as (
    select
        'barometr_zawodow' as source_system,
        v.area_code as source_code,
        'TERC:' || v.area_code as geography_key
    from ({{ poc_barometr_zawodow_voivodeships() }}) as v
    union all
    select
        'barometr_zawodow' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
