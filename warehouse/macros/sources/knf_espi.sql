{# KNF ESPI issuer reports (gold.build_knf_espi, #1486): one block; Poland only (the list is the regulated
   market's), so no geography macro (the build stage writes PL). D34 registration (docs/source-registration.md). #}
{% macro poc_source__knf_espi() %}
    {{ return({
        'block_model': 'poc_build_knf_espi',
        'blocks': [['reports', 'ESPI issuer reports received by KNF (OAM list)']],
    }) }}
{% endmacro %}
