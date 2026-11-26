{# kgpsp_interventions (register EX-003): Gold registration, decision D34 (docs/source-registration.md). Blocks
   'incidents' (table 1) and 'casualties' (table 10) of models/gold/poc_build_kgpsp_interventions.sql. Areas: the
   two-digit TERYT voivodeship code (silver.kgpsp_interventions_counts voivodeship_code), matched to dim_geography's
   terc_code; 'PL' = Poland. A static list from geo, so the shared map reads no Silver table of this source. #}
{% macro poc_source__kgpsp_interventions() %}
    {{ return({'block_model': 'poc_build_kgpsp_interventions', 'blocks': [
        ['incidents', 'KG PSP incidents by type and size'],
        ['casualties', 'KG PSP casualties of fires and local hazards'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__kgpsp_interventions() %}
kgpsp_interventions as (
    select
        'kgpsp_interventions' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'kgpsp_interventions' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
