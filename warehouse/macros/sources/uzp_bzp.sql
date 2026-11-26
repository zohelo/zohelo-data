{# uzp_bzp (register PLG-022): Gold registration, decision D34 (docs/source-registration.md). Block 'notices'
   (models/gold/poc_build_uzp_bzp.sql): monthly counts of BZP contract and result notices and the value of the
   contracts their result notices report. Areas: 'POL' (every notice: BZP is the Polish national bulletin) = Poland;
   the contracting authority's voivodeship as published (SL.MT.007: 'PL' + the TERYT voivodeship code, 'PL14' =
   mazowieckie) = 'TERC:<code>', from the static NUTS list read through the helper CTE uzp_bzp_nuts_pl (one
   'wojewodztwo' row per voivodeship code; poc_map_geography has no CTE named nuts_pl). #}
{% macro poc_source__uzp_bzp() %}
    {{ return({
        'block_model': 'poc_build_uzp_bzp',
        'blocks': [['notices', 'BZP contract and result notices (monthly counts and contract values)']],
    }) }}
{% endmacro %}

{% macro poc_geography__uzp_bzp() %}
uzp_bzp_nuts_pl as ({{ poc_nuts_pl() }}),

uzp_bzp as (
    select
        'uzp_bzp' as source_system,
        'PL' || n.woj_code as source_code,
        'TERC:' || n.woj_code as geography_key
    from uzp_bzp_nuts_pl as n
    where n.level_name = 'wojewodztwo'
    union all
    select
        'uzp_bzp' as source_system,
        'POL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
