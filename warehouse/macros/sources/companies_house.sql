{# UK Companies House Free Company Data Product (gold.build_companies_house, #1502): D34 registration
   (docs/source-registration.md). No geography macro: the build stage sets GB itself (the source has no area
   codes; the country of origin is a breakdown). #}
{% macro poc_source__companies_house() %}
    {{ return({
        'block_model': 'poc_build_companies_house',
        'blocks': [
            ['basic_company_data', 'Companies House Free Company Data Product (monthly snapshot)'],
        ],
    }) }}
{% endmacro %}
