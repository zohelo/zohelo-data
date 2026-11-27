{# pig_midas (register PL-ENV-015): Gold registration, decision D34 (docs/source-registration.md). Blocks 'deposits'
   and 'mining_areas' (models/gold/poc_build_pig_midas.sql): snapshot counts of the MIDAS layers for Poland and the
   voivodeships. The WFS gives no territory attribute; the build places each contour's point on its surface in a PRG
   voivodeship boundary (silver.prg_units), as build_imgw_archive does for stations. Source codes: 'PL' and the
   voivodeship's 2-digit TERYT code. #}
{% macro poc_source__pig_midas() %}
    {{ return({'block_model': 'poc_build_pig_midas', 'blocks': [
        ['deposits', 'PIG-PIB MIDAS documented mineral deposits'],
        ['mining_areas', 'PIG-PIB MIDAS mining areas'],
    ]}) }}
{% endmacro %}

{% macro poc_geography__pig_midas() %}
pig_midas as (
    select
        'pig_midas' as source_system,
        g.terc_code as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
    union all
    select
        'pig_midas' as source_system,
        'PL' as source_code,
        'PL' as geography_key
)
{% endmacro %}

{# Text of a MIDAS property: trimmed, empty text and the producer's literal 'null' as NULL. #}
{% macro poc_pig_midas_text(column) -%}
    nullif(nullif(trim({{ column }}), ''), 'null')
{%- endmacro %}

{# A whole number written as an integer or as a float with a zero fraction ('17350.0'); anything else fails
   the build. #}
{% macro poc_pig_midas_integer(column, model) -%}
    case
        when {{ poc_pig_midas_text(column) }} is null then null
        when regexp_full_match(trim({{ column }}), '[0-9]+(\.0+)?')
            then cast(cast(trim({{ column }}) as decimal(18, 3)) as bigint)
        else error('{{ model }}: not a whole number in {{ column }}: ' || {{ column }})
    end
{%- endmacro %}

{# A date written dd/mm/yyyy; anything else but NULL fails the build. #}
{% macro poc_pig_midas_date(column, model) -%}
    case
        when {{ poc_pig_midas_text(column) }} is null then null
        when regexp_full_match(trim({{ column }}), '[0-9]{2}/[0-9]{2}/[0-9]{4}')
            then cast(strptime(trim({{ column }}), '%d/%m/%Y') as date)
        else error('{{ model }}: not a dd/mm/yyyy date in {{ column }}: ' || {{ column }})
    end
{%- endmacro %}

{# Centroid and a point on the surface (always inside the contour) of the GeoJSON geometry, in degrees (EPSG:4326);
   an invalid contour is made valid first. Needs the spatial extension (pre_hook of the model). #}
{% macro poc_pig_midas_points(column) -%}
    {%- set g -%}
        case
            when st_isvalid(st_geomfromgeojson({{ column }})) then st_geomfromgeojson({{ column }})
            else st_makevalid(st_geomfromgeojson({{ column }}))
        end
    {%- endset -%}
    round(st_x(st_centroid({{ g }})), 6) as centroid_lon,
    round(st_y(st_centroid({{ g }})), 6) as centroid_lat,
    round(st_x(st_pointonsurface({{ g }})), 6) as point_lon,
    round(st_y(st_pointonsurface({{ g }})), 6) as point_lat
{%- endmacro %}

{# Silver SQL shared by silver.pig_midas_mining_areas and silver.pig_midas_mining_terrains: one row per contour of the
   newest copy of the current and the archival layer. #}
{% macro poc_pig_midas_areas(current, archival, model) %}
with newest as (
    select *
    from {{ source('pig_midas', 'pig_midas_features') }}
    where entity in ('{{ current }}', '{{ archival }}')
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity as layer,
    {{ poc_pig_midas_integer('id_konturu', model) }} as contour_id,
    {{ poc_pig_midas_integer('objectid', model) }} as objectid,
    {{ poc_pig_midas_text('nazwa') }} as name,
    {{ poc_pig_midas_text('status') }} as status,
    {{ poc_pig_midas_text('numer_w_rejestrze') }} as register_number,
    {{ poc_pig_midas_date('data_ustanowienia', model) }} as established_on,
    {{ poc_pig_midas_date('data_waznosci', model) }} as valid_until,
    {{ poc_pig_midas_text('decyzja_wyznaczajaca') }} as designating_decision,
    {{ poc_pig_midas_text('wydawca_decyzji_wyznaczajacej') }} as designating_authority,
    cast({{ poc_pig_midas_text('powierzchnia_m2') }} as double) as legal_area_m2,
    {{ poc_pig_midas_text('nadzor_oug') }} as mining_office,
    {{ poc_pig_midas_text('komentarz') }} as comment,
    {{ poc_pig_midas_text('decyzja_znoszaca') }} as abolishing_decision,
    {{ poc_pig_midas_text('wydawca_decyzji_znoszacej') }} as abolishing_authority,
    {{ poc_pig_midas_date('data_zniesienia', model) }} as abolished_on,
    {{ poc_pig_midas_integer('numer_zloza', model) }} as deposit_number,
    {{ poc_pig_midas_text('nazwa_zloza') }} as deposit_name,
    {{ poc_pig_midas_text('kopalina') }} as mineral,
    {{ poc_pig_midas_text('uklad_wspolrzednych') }} as coordinate_system,
    {{ poc_pig_midas_text('arkusz_mapy') }} as map_sheet,
    cast({{ poc_pig_midas_text('shape_area_m2') }} as double) as shape_area_m2,
    {{ poc_pig_midas_points("nullif(geometry_geojson, 'null')") }},
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as snapshot_date,
    source_version,
    raw_sha256
from newest
{% endmacro %}
