{# gis_sanitary (register PLS-018, #442): Gold registration, decision D34 (docs/source-registration.md). Block
   'food_samples' (models/gold/poc_build_gis_sanitary.sql). The table is national: one static code, PL. #}
{% macro poc_source__gis_sanitary() %}
    {{ return({'block_model': 'poc_build_gis_sanitary', 'blocks': [['food_samples', 'GIS food samples examined and disqualified (annual)']]}) }}
{% endmacro %}

{% macro poc_geography__gis_sanitary() %}
gis_sanitary as (
    select
        'gis_sanitary' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
