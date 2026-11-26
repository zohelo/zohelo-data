{# Work list of the Council of Ministers (register PLG-034, #491): Gold block drafts_published
   (models/gold/poc_build_kprm_work_list.sql). D34 registration (docs/source-registration.md). #}
{% macro poc_source__kprm_work_list() %}
    {{ return({
        'block_model': 'poc_build_kprm_work_list',
        'blocks': [['drafts_published', 'Government drafts published on the work list of the Council of Ministers']],
    }) }}
{% endmacro %}

{% macro poc_geography__kprm_work_list() %}
-- Work list of the Council of Ministers: a national list, one area (Poland); a static list, so the map never waits
-- for the source's Silver table.
kprm_work_list as (
    select
        'kprm_work_list' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 = 'PL'
)
{% endmacro %}
