{# RAD-on / POL-on (register PLS-023; gold.build_radon, gold.radon_institution, gold.radon_study_programme, #2990):
   D34 registration (docs/source-registration.md). Two blocks: operating institutions and study programmes in
   operation, counted on the day of the newest download, by voivodeship. #}
{% macro poc_source__radon() %}
    {{ return({
        'block_model': 'poc_build_radon',
        'blocks': [
            ['institutions', 'RAD-on institutions of higher education and science (POL-on)'],
            ['study_programmes', 'RAD-on study programmes (POL-on)'],
        ],
    }) }}
{% endmacro %}

{# POL-on prints the voivodeship as its lower-case name (mazowieckie): mapped through the names of dim_geography,
   never through the source's own table. #}
{% macro poc_geography__radon() %}
radon as (
    select
        'radon' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
)
{% endmacro %}
