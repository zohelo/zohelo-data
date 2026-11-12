{# NFZ statistics API, services (gold.build_nfz_services, #2929): one block per catalog. D34 registration
   (docs/source-registration.md). #}
{% macro poc_nfz_services_blocks() %}
    select * from (
        values
        ('1a', 'NFZ JGP statistics: homogeneous patient groups (catalog 1a)'),
        ('1b', 'NFZ JGP statistics: separate benefits (catalog 1b)'),
        ('1c', 'NFZ JGP statistics: benefits added up (catalog 1c)'),
        ('1d', 'NFZ JGP statistics: radiotherapy (catalog 1d)'),
        ('1w', 'NFZ JGP statistics: highly specialised benefits (catalog 1w)')
    ) as t (block_id, block_name)
{% endmacro %}

{# The basic-data fields built into Gold: API field, measure slug, unit code as published, unit name, label (PL).
   scope 'all' = every loaded benefit, Poland only; 'detail' = only the benefits of poc_nfz_services_detail(),
   Poland and the 16 branches. Values are never rescaled or rounded (D33; the terms forbid modifying the data). #}
{% macro poc_nfz_services_measures() %}
    select * from (
        values
        ('number-of-hospitalizations', 'hospitalisations', 'hospitalisations', 'hospitalisations', 'liczba hospitalizacji', 'all'),
        ('number-of-patients', 'patients', 'persons', 'persons', 'liczba pacjentów', 'all'),
        ('ratio-of-rehospitalizations', 'hospitalisations_per_patient', 'hospitalisations_per_patient', 'hospitalisations per patient', 'liczba hospitalizacji na pacjenta', 'detail'),
        ('percentage', 'percentage', 'pct', '%', 'udział procentowy (percentage)', 'detail'),
        ('percentage-of-sections', 'percentage_of_section', 'pct', '%', 'udział procentowy w sekcji (percentage-of-sections)', 'detail'),
        ('duration-of-hospitalization-mediana', 'stay_median', 'days', 'days', 'mediana czasu pobytu', 'detail'),
        ('duration-of-hospitalization-mode', 'stay_mode', 'days', 'days', 'dominanta czasu pobytu', 'detail'),
        ('average-value-of-hospitalization', 'value_per_stay', 'PLN', 'PLN', 'średnia wartość hospitalizacji', 'detail'),
        ('average-value-of-hospitalization-points', 'value_per_stay_points', 'points', 'NFZ points', 'średnia wartość hospitalizacji w punktach', 'detail'),
        ('average-value-of-drg', 'value_per_group', 'PLN', 'PLN', 'średnia wartość JGP', 'detail'),
        ('average-value-of-drg-points', 'value_per_group_points', 'points', 'NFZ points', 'średnia wartość JGP w punktach', 'detail')
    ) as t (api_field, measure_slug, unit_code, unit_name, measure_label, scope)
{% endmacro %}

{# Benefits built with every measure, for Poland and by NFZ branch: the JGP groups of the KPIs (kpi_measure.csv),
   by NFZ product code, never by name. #}
{% macro poc_nfz_services_detail() %}
    select * from (
        values
        ('1a', '5.51.01.0013001'),  -- N01 childbirth
        ('1a', '5.51.01.0008013'),  -- H13 primary total hip replacement
        ('1a', '5.51.01.0008015'),  -- H15 primary total knee replacement
        ('1a', '5.51.01.0001048')   -- A48 stroke, stroke unit > 7 days
    ) as t (catalog, benefit_code)
{% endmacro %}

{% macro poc_source__nfz_services() %}
    {{ return({
        'block_model': 'poc_build_nfz_services',
        'activated': "select 'nfz_services', block_id, block_name from (" ~ poc_nfz_services_blocks() ~ ")",
    }) }}
{% endmacro %}

{% macro poc_geography__nfz_services() %}
-- NFZ statistics API, services: 'PL' = the Poland answer (branch=false); the 16 NFZ regional branches (oddziały
-- wojewódzkie) by the API's branch code, numbered alphabetically 01 dolnośląski … 16 zachodniopomorski, each the
-- voivodeship of the same name: TERYT code = 2 × branch code. A static list, so the map never waits for the source.
nfz_services as (
    select
        'nfz_services' as source_system,
        b.source_code,
        b.geography_key
    from (
        values
        ('PL', 'PL'),
        ('01', 'TERC:02'), ('02', 'TERC:04'), ('03', 'TERC:06'), ('04', 'TERC:08'),
        ('05', 'TERC:10'), ('06', 'TERC:12'), ('07', 'TERC:14'), ('08', 'TERC:16'),
        ('09', 'TERC:18'), ('10', 'TERC:20'), ('11', 'TERC:22'), ('12', 'TERC:24'),
        ('13', 'TERC:26'), ('14', 'TERC:28'), ('15', 'TERC:30'), ('16', 'TERC:32')
    ) as b (source_code, geography_key)
)
{% endmacro %}
