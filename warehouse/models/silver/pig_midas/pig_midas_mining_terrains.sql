{{ config(pre_hook="install spatial; load spatial") }}
{# PIG-PIB MIDAS (register PL-ENV-015), mining terrains (tereny górnicze) from bronze.pig_midas_features: one row per
   contour of the newest copy of the WFS layers 'MIDAS - tereny górnicze' (layer mining_terrains, status 'aktualny')
   and 'MIDAS - archiwalne tereny górnicze (wyselekcjonowane dane archiwalne)' (layer mining_terrains_archival, status
   'zniesiony'). Grain: layer x contour_id (ID_konturu). Same columns as silver.pig_midas_mining_areas (shared SQL
   poc_pig_midas_areas, macros/sources/pig_midas.sql); the geometry stays in Bronze. #}
{{ poc_pig_midas_areas('mining_terrains', 'mining_terrains_archival', 'pig_midas_mining_terrains') }}
