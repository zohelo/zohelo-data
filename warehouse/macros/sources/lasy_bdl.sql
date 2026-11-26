{# lasy_bdl (register PL-ENV-018): Gold registration, decision D34 (docs/source-registration.md). Block 'districts'
   (models/gold/poc_build_lasy_bdl.sql): snapshot count and area of the State Forests' forest districts for Poland, the
   voivodeships and per regional directorate (RDLP). The WFS gives no territory attribute; the build places each
   district's point on its surface in a PRG gmina boundary (silver.prg_units), as build_pig_midas does. Source codes:
   'PL' and the voivodeship's 2-digit TERYT code. #}
{% macro poc_source__lasy_bdl() %}
    {{ return({'block_model': 'poc_build_lasy_bdl', 'blocks': [
        ['districts', 'Lasy Państwowe BDL forest districts'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__lasy_bdl() %}
lasy_bdl as (
    select
        'lasy_bdl' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'lasy_bdl' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}

{# Text of a BDL property: trimmed, empty text as NULL. #}
{% macro poc_lasy_bdl_text(column) -%}
    nullif(trim({{ column }}), '')
{%- endmacro %}

{# A two-digit code (RDLP region_cd, district inspectorate_cd); anything else fails the build. #}
{% macro poc_lasy_bdl_code(column, model) -%}
    case
        when regexp_full_match(coalesce({{ column }}, ''), '[0-9]{2}') then {{ column }}
        else error('{{ model }}: not a two-digit code in {{ column }}: ' || coalesce({{ column }}, 'NULL'))
    end
{%- endmacro %}

{# The data year a_year (four digits); anything else fails the build. #}
{% macro poc_lasy_bdl_year(column, model) -%}
    case
        when regexp_full_match(coalesce({{ column }}, ''), '[0-9]{4}') then cast({{ column }} as integer)
        else error('{{ model }}: not a year in {{ column }}: ' || coalesce({{ column }}, 'NULL'))
    end
{%- endmacro %}

{# The values Bronze computed from the geometry: planar area in EPSG:2180 (m²), centroid and point on the surface
   (degrees, EPSG:4326). #}
{% macro poc_lasy_bdl_measures() -%}
    cast(area_m2 as double) as area_m2,
    cast(centroid_lon as double) as centroid_lon,
    cast(centroid_lat as double) as centroid_lat,
    cast(point_lon as double) as point_lon,
    cast(point_lat as double) as point_lat
{%- endmacro %}
