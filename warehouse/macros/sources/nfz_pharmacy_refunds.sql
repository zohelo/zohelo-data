{# NFZ pharmacy refunds (register PLS-013, #2930): the areas and ATC selections landed (entities <area>_<atc> of
   ingestion/to_landing/nfz_pharmacy_refunds/extract.py), one Gold block, and the geography: NFZ voivodeship codes
   01..16 (alphabetical, NFZ order) → TERYT voivodeship (TERC:02 ... TERC:32), PL = no province filter. Static
   lists, so the geography map never waits for the source's Silver table. #}
{% macro poc_nfz_pharmacy_refunds_areas() %}
    select * from (
        values
        ('PL', 'PL', 'Polska'), ('01', 'TERC:02', 'dolnośląskie'), ('02', 'TERC:04', 'kujawsko-pomorskie'),
        ('03', 'TERC:06', 'lubelskie'), ('04', 'TERC:08', 'lubuskie'), ('05', 'TERC:10', 'łódzkie'),
        ('06', 'TERC:12', 'małopolskie'), ('07', 'TERC:14', 'mazowieckie'), ('08', 'TERC:16', 'opolskie'),
        ('09', 'TERC:18', 'podkarpackie'), ('10', 'TERC:20', 'podlaskie'), ('11', 'TERC:22', 'pomorskie'),
        ('12', 'TERC:24', 'śląskie'), ('13', 'TERC:26', 'świętokrzyskie'), ('14', 'TERC:28', 'warmińsko-mazurskie'),
        ('15', 'TERC:30', 'wielkopolskie'), ('16', 'TERC:32', 'zachodniopomorskie')
    ) as t (area_code, geography_key, area_name)
{% endmacro %}

{# The entities landed: every area x (no ATC filter 'T' + the 14 ATC main groups of /atc). #}
{% macro poc_nfz_pharmacy_refunds_entities() %}
    select a.area_code || '_' || c.atc as entity
    from ({{ poc_nfz_pharmacy_refunds_areas() }}) as a
    cross join (
        values ('T'), ('A'), ('B'), ('C'), ('D'), ('G'), ('H'), ('J'), ('L'), ('M'), ('N'), ('P'), ('R'), ('S'), ('V')
    ) as c (atc)
{% endmacro %}

{# The five values of /monthly-provisions: one measure each, unit as published (PLN in złoty with grosze; packages
   as a count with fractions). Definitions: warehouse/models/gold/_nfz_pharmacy_refunds.yml. #}
{% macro poc_nfz_pharmacy_refunds_measures() %}
    select * from (
        values
        ('quantity', 'packages', 'packages', 'Reimbursed packages dispensed by pharmacies (NFZ)'),
        ('refund', 'PLN', 'PLN', 'NFZ refund for medicines dispensed by pharmacies (NFZ)'),
        ('donation', 'PLN', 'PLN', 'Ministry of Health subsidy, 75+ programme, pharmacy medicines (NFZ)'),
        ('contribution_of_patient', 'PLN', 'PLN', 'Patient co-payment for reimbursed medicines (NFZ)'),
        ('value', 'PLN', 'PLN', 'Total cost of reimbursed medicines dispensed by pharmacies (NFZ)')
    ) as t (field, unit_code, unit_name, measure_name)
{% endmacro %}

{# D34 registration (docs/source-registration.md). #}
{% macro poc_source__nfz_pharmacy_refunds() %}
    {{ return({
        'block_model': 'poc_build_nfz_pharmacy_refunds',
        'blocks': [['monthly_provisions', 'NFZ pharmacy refunds, monthly, by voivodeship and ATC group']],
    }) }}
{% endmacro %}

{% macro poc_geography__nfz_pharmacy_refunds() %}
-- NFZ pharmacy refunds: PL and the NFZ voivodeship codes 01..16 (static list of poc_nfz_pharmacy_refunds_areas).
nfz_pharmacy_refunds as (
    select
        'nfz_pharmacy_refunds' as source_system,
        area_code as source_code,
        geography_key
    from ({{ poc_nfz_pharmacy_refunds_areas() }})
)
{% endmacro %}
