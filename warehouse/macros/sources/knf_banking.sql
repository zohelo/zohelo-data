{# knf_banking (register PLG-017): Gold registration, decision D34 (docs/source-registration.md). Block
   'banking_sector' (models/gold/poc_build_knf_banking.sql). Poland only: the build stage writes geography 'PL'
   itself, so no geography macro. #}
{% macro poc_source__knf_banking() %}
    {{ return({'block_model': 'poc_build_knf_banking', 'blocks': [['banking_sector', 'KNF monthly banking-sector data']]}) }}
{% endmacro %}
