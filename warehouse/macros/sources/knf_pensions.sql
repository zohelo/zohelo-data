{# knf_pensions (register PLG-018): Gold registration, decision D34 (docs/source-registration.md). Block
   'ppk_net_assets' (models/gold/poc_build_knf_pensions.sql). Poland only: the build stage writes geography 'PL'
   itself, so no geography macro. #}
{% macro poc_source__knf_pensions() %}
    {{ return({'block_model': 'poc_build_knf_pensions', 'blocks': [['ppk_net_assets', 'KNF PPK net asset value']]}) }}
{% endmacro %}
