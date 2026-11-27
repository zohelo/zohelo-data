{# GIOŚ groundwater quality monitoring (register PL-ENV-012, #2870): Gold registration, decision D34
   (docs/source-registration.md). Block 'nitrate' (models/gold/poc_build_gios_groundwater.sql): monitoring points with
   nitrate results and those above 50 mg/l NO3, per year and monitoring kind, Poland and voivodeships. #}
{% macro poc_source__gios_groundwater() %}
    {{ return({'block_model': 'poc_build_gios_groundwater', 'blocks': [
        ['nitrate', 'GIOŚ groundwater monitoring: points above 50 mg/l nitrate']
    ]}) }}
{% endmacro %}

{# The result files print the point's voivodeship as its lower-case name (wielkopolskie; no TERYT code): mapped
   through the names of dim_geography, never through the source's own table; Poland is built as 'PL' directly. A name
   outside the 16 fails the accepted_values test of silver.gios_groundwater_points first. #}
{% macro poc_geography__gios_groundwater() %}
gios_groundwater as (
    select
        'gios_groundwater' as source_system,
        lower(g.name_pl) as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
)
{% endmacro %}
