{# NBP balance of payments and international investment position (gold.build_nbp_bop, #2780): one block per
   workbook; Poland only, so no geography macro (the build stage writes PL). D34 registration
   (docs/source-registration.md). #}
{% macro poc_source__nbp_bop() %}
    {{ return({
        'block_model': 'poc_build_nbp_bop',
        'blocks': [
            ['bop_m', 'NBP balance of payments, monthly'],
            ['bop_q', 'NBP balance of payments, quarterly'],
            ['iip_q', 'NBP international investment position, quarterly'],
        ],
    }) }}
{% endmacro %}
