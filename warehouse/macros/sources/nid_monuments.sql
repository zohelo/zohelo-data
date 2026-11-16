{# nid_monuments (register PLS-026): Gold registration, decision D34 (docs/source-registration.md). Block 'register'
   (models/gold/poc_build_nid_monuments.sql): counts of the NID register of immovable monuments by powiat,
   voivodeship and Poland. The lists name the powiat in each page header ("woj. opolskie – pow. brzeski",
   "pow. miejski Opole", "pow. grodzki Bydgoszcz", "pow. krakowski miejski"; the Warsaw list by district), never by
   code, so the powiat is matched by name within its voivodeship to the TERYT powiat names of dim_geography (a
   reported fallback: an unmatched name maps to _X and is counted by build_check). Source codes:
   '<voivodeship TERC>|<normalised powiat name>', 'woj:<voivodeship TERC>', 'PL'. #}
{% macro poc_source__nid_monuments() %}
    {{ return({'block_model': 'poc_build_nid_monuments', 'blocks': [['register', 'NID register of immovable monuments (register book A)']]}) }}
{% endmacro %}

{# The powiat source code of a list item: voivodeship code and the powiat name as printed, lower case, without the
   "miejski"/"grodzki" of a city with powiat rights, hyphens as spaces (TERYT "Jastrzębie-Zdrój", NID "Jastrzębie
   Zdrój"); Kraków is printed "krakowski miejski". #}
{% macro poc_nid_monuments_area_code(woj_code, powiat_text) %}
    ({{ woj_code }} || '|' || case
        when lower(trim({{ powiat_text }})) = 'krakowski miejski' then 'kraków'
        else trim(regexp_replace(
            regexp_replace(lower(trim({{ powiat_text }})), '^(miejski|grodzki)\s+', ''), '[-\s]+', ' ', 'g'
        ))
    end)
{% endmacro %}

{% macro poc_geography__nid_monuments() %}
nid_monuments as (
    select
        'nid_monuments' as source_system,
        substr(g.terc_code, 1, 2) || '|' || trim(regexp_replace(lower(g.name_pl), '[-\s]+', ' ', 'g')) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'powiat'
    union all
    select
        'nid_monuments' as source_system,
        'woj:' || g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'nid_monuments' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
