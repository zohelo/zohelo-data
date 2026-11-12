{# NBP official interest rates (gold.build_nbp_interest_rates, #2737): one block; Poland only, so no geography
   macro (the build stage writes PL). D34 registration (docs/source-registration.md). #}
{% macro poc_source__nbp_interest_rates() %}
    {{ return({
        'block_model': 'poc_build_nbp_interest_rates',
        'blocks': [['policy_rates', 'NBP official interest rates']],
    }) }}
{% endmacro %}
