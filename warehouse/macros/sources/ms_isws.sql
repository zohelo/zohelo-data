{# ms_isws (register PLG-031): Gold registration, decision D34 (docs/source-registration.md). Block 'multi_year'
   (models/gold/poc_build_ms_isws.sql); every table is national, so the one code 'PL' maps to Poland (a static
   list: the map never waits for the source's Silver table). #}
{% macro poc_source__ms_isws() %}
    {{ return({'block_model': 'poc_build_ms_isws', 'blocks': [['multi_year', 'ISWS multi-year tables (courts, adoptions, notarial acts)']]}) }}
{% endmacro %}

{% macro poc_geography__ms_isws() %}
ms_isws as (
    select
        'ms_isws' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
