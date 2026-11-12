{# xbrl_filings (register CD-013): Gold registration, decision D34 (docs/source-registration.md). Block
   'esef_reports' (models/gold/poc_build_xbrl_filings.sql): counts for Poland only, geography PL set in the build
   stage, so no geography macro. #}
{% macro poc_source__xbrl_filings() %}
    {{ return({'block_model': 'poc_build_xbrl_filings', 'blocks': [['esef_reports', 'ESEF annual financial reports held by filings.xbrl.org']]}) }}
{% endmacro %}
