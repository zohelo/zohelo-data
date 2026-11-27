{# pzh_epimeld (register PLS-019): Gold registration, decision D34 (docs/source-registration.md). Block 'annual'
   (models/gold/poc_build_pzh_epimeld.sql). Areas: 'PL' (the bulletins' POLSKA row) and the TERYT voivodeship codes
   (the bulletins number voivodeships 1-16 alphabetically; Silver turns number n into code 2n). A static list from
   geo, so the map never waits for the source's Silver table. #}
{% macro poc_source__pzh_epimeld() %}
    {{ return({'block_model': 'poc_build_pzh_epimeld', 'blocks': [['annual', 'NIZP PZH-PIB infectious diseases: cases and incidence by disease (annual bulletins)']]}) }}
{% endmacro %}

{% macro poc_geography__pzh_epimeld() %}
pzh_epimeld as (
    select
        'pzh_epimeld' as source_system,
        left(g.terc_code, 2) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'pzh_epimeld' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
