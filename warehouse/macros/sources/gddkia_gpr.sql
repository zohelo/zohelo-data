{# gddkia_gpr (register PL-ENV-022): Gold registration, decision D34 (docs/source-registration.md). Block 'sdrr'
   (models/gold/poc_build_gddkia_gpr.sql). Areas: the files' "Nr woj." is the two-digit TERYT voivodeship code
   (lpad in silver.gddkia_gpr_sdrr), matched to dim_geography's terc_code; 'PL' = Poland. A static list from geo, so the
   shared map reads no Silver table of this source (no poc_optional_ref needed). #}
{% macro poc_source__gddkia_gpr() %}
    {{ return({'block_model': 'poc_build_gddkia_gpr', 'blocks': [['sdrr', 'GDDKiA traffic census: average annual daily traffic (SDRR)']]}) }}
{% endmacro %}

{% macro poc_geography__gddkia_gpr() %}
gddkia_gpr as (
    select
        'gddkia_gpr' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'gddkia_gpr' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}
