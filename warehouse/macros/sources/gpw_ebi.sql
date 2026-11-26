{# GPW EBI issuer reports (gold.build_gpw_ebi, #1488): one block; Poland only (NewConnect and Catalyst issuers'
   reports on GPW's list), so no geography macro (the build stage writes PL). D34 registration
   (docs/source-registration.md). #}
{% macro poc_source__gpw_ebi() %}
    {{ return({
        'block_model': 'poc_build_gpw_ebi',
        'blocks': [['reports', 'EBI issuer reports published by GPW']],
    }) }}
{% endmacro %}
