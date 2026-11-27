{# PIG-PIB CBDG boreholes (register PL-ENV-014), from bronze.pig_boreholes_features: one row per borehole of the newest
   download of the WFS layer 'Otwory_wiertnicze_CBDG' (Central Geological Database, subsystem boreholes). Grain:
   id_cbdg (the CBDG borehole id; OBJECTID, the feature's number in the layer, is unique too). Name, depth, stratigraphy,
   drilling purpose and year, elevation, coordinates as published (geographic in DDDMMSS.ss text, PL-1992 metres),
   location basis, locality, gmina, powiat and voivodeship names, archive, inventory number, core store and the
   geophysics, analyses and studies fields, Polish and English labels as published; the point in degrees (EPSG:4326)
   from the geometry. Text 'null' and empty text are NULL. Bronze holds only the pages of the newest download (bronze
   latest-only prune); a page whose bytes did not change between downloads keeps the version it was first loaded with,
   so every row reads the newest version as its snapshot. #}
with bronze as (
    select
        *,
        max(source_version) over () as snapshot_version
    from {{ source('pig_boreholes', 'pig_boreholes_features') }}
)

select
    {{ poc_pig_boreholes_integer('id_cbdg', 'pig_boreholes_boreholes') }} as id_cbdg,
    {{ poc_pig_boreholes_integer('objectid', 'pig_boreholes_boreholes') }} as objectid,
    {{ poc_pig_boreholes_text('nazwa') }} as borehole_name,
    {{ poc_pig_boreholes_number('glebokosc_m', 'pig_boreholes_boreholes') }} as depth_m,
    {{ poc_pig_boreholes_text('stratygrafia') }} as stratigraphy,
    {{ poc_pig_boreholes_text('stratigraphy') }} as stratigraphy_en,
    {{ poc_pig_boreholes_text('cel_wiercenia') }} as drilling_purpose,
    {{ poc_pig_boreholes_text('drilling_purpose') }} as drilling_purpose_en,
    {{ poc_pig_boreholes_integer('rok_wiercenia', 'pig_boreholes_boreholes') }} as drilling_year,
    {{ poc_pig_boreholes_number('rzedna_m_npm', 'pig_boreholes_boreholes') }} as elevation_m_asl,
    {{ poc_pig_boreholes_text('dlugosc_geograficzna') }} as longitude_dms,
    {{ poc_pig_boreholes_text('szerokosc_geograficzna') }} as latitude_dms,
    {{ poc_pig_boreholes_number('x_pl1992', 'pig_boreholes_boreholes') }} as x_pl1992,
    {{ poc_pig_boreholes_number('y_pl1992', 'pig_boreholes_boreholes') }} as y_pl1992,
    {{ poc_pig_boreholes_text('podstawa_lokalizacji') }} as location_basis,
    {{ poc_pig_boreholes_text('location_basis') }} as location_basis_en,
    {{ poc_pig_boreholes_text('miejscowosc') }} as locality,
    {{ poc_pig_boreholes_text('gmina') }} as gmina_name,
    {{ poc_pig_boreholes_text('powiat') }} as powiat_name,
    {{ poc_pig_boreholes_text('wojewodztwo') }} as voivodeship_name,
    {{ poc_pig_boreholes_text('archiwum') }} as archive,
    {{ poc_pig_boreholes_text('nr_inwentarzowy') }} as inventory_number,
    {{ poc_pig_boreholes_text('rdzen') }} as core_store,
    {{ poc_pig_boreholes_text('geofizyka') }} as geophysics,
    {{ poc_pig_boreholes_text('analizy') }} as analyses,
    {{ poc_pig_boreholes_text('badania') }} as studies,
    {{ poc_pig_boreholes_text('research') }} as studies_en,
    {{ poc_pig_boreholes_number('point_lon', 'pig_boreholes_boreholes') }} as point_lon,
    {{ poc_pig_boreholes_number('point_lat', 'pig_boreholes_boreholes') }} as point_lat,
    cast(strptime(snapshot_version, '%Y%m%dT%H%M%SZ') as date) as snapshot_date,
    snapshot_version as source_version,
    raw_sha256
from bronze
