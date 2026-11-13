{# ted_notices (register PLG-023): Gold registration, decision D34 (docs/source-registration.md). Block 'notices'
   (models/gold/poc_build_ted_notices.sql): monthly counts of TED notices of Polish buyers. Areas: 'POL' (the
   buyer country of every notice loaded) = Poland; the buyer's NUTS code (eForms BT-507) reduced to its NUTS 2
   region, and PL91/PL92 to 'PL9', matched to the voivodeships of poc_nuts_pl. #}
{% macro poc_source__ted_notices() %}
    {{ return({'block_model': 'poc_build_ted_notices', 'blocks': [['notices', 'TED notices of Polish buyers (monthly counts)']]}) }}
{% endmacro %}

{% macro poc_geography__ted_notices() %}
ted_notices as (
    select
        'ted_notices' as source_system,
        n.nuts_code as source_code,
        'TERC:' || n.woj_code as geography_key
    from nuts_pl as n
    where n.level_name = 'wojewodztwo'
    union all
    select
        'ted_notices' as source_system,
        'POL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
