{# KPRM work list (register PLG-034, #491): one Gold block, drafts entering the Council of Ministers' work list
   (gold.build_kprm_work_list). D34 registration (docs/source-registration.md). #}
{% macro poc_source__kprm_work_list() %}
    {{ return({
        'block_model': 'poc_build_kprm_work_list',
        'blocks': [['entries', 'Drafts entering the Council of Ministers work list (KPRM)']],
    }) }}
{% endmacro %}

{% macro poc_geography__kprm_work_list() %}
-- KPRM work list: a list of the Polish government, one area (Poland); a static list, so the map never waits for the
-- source's Silver table.
kprm_work_list as (
    select
        'kprm_work_list' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
