{{ config(pre_hook="install spatial; load spatial") }}
{# PIG-PIB MIDAS (register PL-ENV-015), mining areas (obszary górnicze) from bronze.pig_midas_features: one row per
   contour of the newest copy of the WFS layers 'MIDAS - obszary górnicze' (layer mining_areas, status 'aktualny') and
   'MIDAS - archiwalne obszary górnicze (wyselekcjonowane dane archiwalne)' (layer mining_areas_archival, status
   'zniesiony'). Grain: layer x contour_id (ID_konturu). Name, register number, legal dates (established, valid until,
   abolished), decisions and authorities, legal area in m2 and the GIS area of the contour as published, deposit and
   mineral, the contour's centroid and a point on its surface (degrees, EPSG:4326); the geometry stays in Bronze.
   Shared SQL: poc_pig_midas_areas (macros/sources/pig_midas.sql). #}
{{ poc_pig_midas_areas('mining_areas', 'mining_areas_archival', 'pig_midas_mining_areas') }}
