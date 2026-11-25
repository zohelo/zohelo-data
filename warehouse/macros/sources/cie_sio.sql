{# cie_sio (register PLS-022, SIO archival education statistics of cie.gov.pl): Gold registration, decision D34
   (docs/source-registration.md), and the voivodeship list the Silver models use. Blocks 'teachers', 'schools',
   'graduates' (models/gold/poc_build_cie_sio.sql). Areas: the two-digit GUS voivodeship id ("Id WojGUS") the
   workbooks print, 'PL' for the Poland rows; mapped to dim_geography's TERYT voivodeships ('TERC:<id>'). #}
{% macro poc_source__cie_sio() %}
    {{ return({'block_model': 'poc_build_cie_sio', 'blocks': [
        ['teachers', 'SIO teachers: persons and full-time posts by professional grade (30 September)'],
        ['schools', 'SIO schools and pupils by school type, category, publicness and governing body (30 September)'],
        ['graduates', 'SIO graduates by school type and pupil category (30 September)'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__cie_sio() %}
cie_sio as (
    select
        'cie_sio' as source_system,
        replace(g.geography_key, 'TERC:', '') as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'cie_sio' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}

{# The 16 voivodeships by GUS id with the name the teachers workbook prints (no id there), compared without
   hyphens and case ("Zachodnio-pomorskie"). A fixed official list (TERYT ids), not a lookup by fuzzy name. #}
{% macro poc_cie_sio_voivodeships() %}
    select * from (
        values
        ('02', 'dolnośląskie'), ('04', 'kujawskopomorskie'), ('06', 'lubelskie'), ('08', 'lubuskie'),
        ('10', 'łódzkie'), ('12', 'małopolskie'), ('14', 'mazowieckie'), ('16', 'opolskie'),
        ('18', 'podkarpackie'), ('20', 'podlaskie'), ('22', 'pomorskie'), ('24', 'śląskie'),
        ('26', 'świętokrzyskie'), ('28', 'warmińskomazurskie'), ('30', 'wielkopolskie'), ('32', 'zachodniopomorskie')
    ) as t (area_code, name_key)
{% endmacro %}

{# Normalised pupil category: the three SIO categories by their code ("Kategoria uczniów (001) Dzieci lub
   młodzież"), from the code where printed, else from the label; anything else stays NULL (not_null test). #}
{% macro poc_cie_sio_category(label) %}
    case
        when regexp_matches({{ label }}, '\((00[123])\)') then regexp_extract({{ label }}, '\((00[123])\)', 1)
        when lower({{ label }}) like 'dzieci%' then '001'
        when lower({{ label }}) like 'doro%' then '002'
        when lower({{ label }}) like 'bez kategorii%' then '003'
    end
{% endmacro %}
