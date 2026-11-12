{# Open KRS API (gold.build_ms_krs, gold.krs_entity, #1362): D34 registration (docs/source-registration.md).
   One block: counts of the entities loaded, by legal form and seat voivodeship. #}
{% macro poc_source__ms_krs() %}
    {{ return({
        'block_model': 'poc_build_ms_krs',
        'blocks': [['lei_selection', 'KRS entities loaded (selection: entities with an LEI)']],
    }) }}
{% endmacro %}

{# The register prints the seat voivodeship as its upper-case name (MAZOWIECKIE): mapped through the names of
   dim_geography, never through the source's own table. #}
{% macro poc_geography__ms_krs() %}
ms_krs as (
    select
        'ms_krs' as source_system,
        upper(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
)
{% endmacro %}
