{# otomoto (register CD-034, #3264): Gold registration, decision D34 (docs/source-registration.md). Block 'insights'
   (models/gold/poc_build_otomoto.sql). The report covers Poland only: the one code 'PL', static, so the shared map
   reads no Silver table of this source. #}
{% macro poc_source__otomoto() %}
    {{ return({'block_model': 'poc_build_otomoto', 'blocks': [['insights', 'OTOMOTO Insights: active listings and traffic by segment']]}) }}
{% endmacro %}

{% macro poc_geography__otomoto() %}
otomoto as (
    select
        'otomoto' as source_system,
        'PL' as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
