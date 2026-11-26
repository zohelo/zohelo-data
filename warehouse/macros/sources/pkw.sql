{# PKW election results (register PLG-029, #486): Gold registration, decision D34 (docs/source-registration.md).
   Blocks = election types (models/gold/poc_build_pkw.sql). Geography from the static TERYT list of geo, never from
   the source's Silver table: voivodeship code WW, powiat WWPP, gmina WWPPGG (PKW prints no type digit: the gmina of
   types 1, 2, 3 or a Warsaw district, type 8; the town and rural parts of an urban-rural gmina, types 4 and 5,
   share the code and are not results areas), voivodeship names of the files without a code column, and the EP 2024
   gmina rows of cities with powiat rights, printed by city and voivodeship name only (the town gmina, type 1, of
   a city powiat, powiat code 61 or higher, matched by name inside the voivodeship: a reported name fallback). #}
{% macro poc_source__pkw() %}
    {{ return({
        'block_model': 'poc_build_pkw',
        'blocks': [
            ['sejm', 'PKW: elections to the Sejm'],
            ['pe', 'PKW: elections to the European Parliament'],
            ['sejmik', 'PKW: elections to the voivodeship assemblies (sejmiki)'],
            ['prezydent', 'PKW: presidential elections'],
        ],
    }) }}
{% endmacro %}

{% macro poc_geography__pkw() %}
pkw_woj as (
    select
        g.terc_code,
        lower(g.name_pl) as name_l,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
),

pkw as (
    select
        'pkw' as source_system,
        'PL' as source_code,
        'PL' as geography_key
    union all
    select
        'pkw' as source_system,
        w.terc_code as source_code,
        w.geography_key
    from pkw_woj as w
    union all
    select
        'pkw' as source_system,
        'woj:' || w.name_l as source_code,
        w.geography_key
    from pkw_woj as w
    union all
    select
        'pkw' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'powiat'
    union all
    select
        'pkw' as source_system,
        substr(g.terc_code, 1, 6) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'gmina' and length(g.terc_code) = 7 and substr(g.terc_code, 7, 1) in ('1', '2', '3', '8')
    union all
    select
        'pkw' as source_system,
        'city:' || w.name_l || ':' || lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    inner join pkw_woj as w on substr(g.terc_code, 1, 2) = w.terc_code
    where
        g.level_name = 'gmina'
        and length(g.terc_code) = 7
        and substr(g.terc_code, 7, 1) = '1'
        and substr(g.terc_code, 3, 2) >= '61'
)
{% endmacro %}
