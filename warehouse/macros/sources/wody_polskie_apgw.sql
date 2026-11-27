{# Wody Polskie aPGW (register PL-ENV-013, #3313): Gold registration, decision D34 (docs/source-registration.md).
   Block 'water_bodies' (models/gold/poc_build_wody_polskie_apgw.sql): surface and groundwater bodies of the river
   basin management plans and their published areas, Poland (by river basin district) and voivodeships. #}
{% macro poc_source__wody_polskie_apgw() %}
    {{ return({'block_model': 'poc_build_wody_polskie_apgw', 'blocks': [
        ['water_bodies', 'Wody Polskie aPGW: water bodies of the river basin management plans']
    ]}) }}
{% endmacro %}

{# Area codes of the build stage: 'PL' for Poland and the 2-digit TERYT voivodeship code that the published voivodeship
   list of a surface water body gives in brackets ('mazowieckie (14)'), mapped to the TERYT voivodeships of
   dim_geography. Static: reads only `geo`, never the source's own Silver tables, so the map does not wait for the
   source's first load (no poc_optional_ref needed). #}
{% macro poc_geography__wody_polskie_apgw() %}
wody_polskie_apgw as (
    select
        'wody_polskie_apgw' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'wody_polskie_apgw' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
