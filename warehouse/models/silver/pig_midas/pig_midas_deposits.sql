{{ config(pre_hook="install spatial; load spatial") }}
{# PIG-PIB MIDAS (register PL-ENV-015), mineral deposits from bronze.pig_midas_features: one row per deposit contour of
   the newest copy of the WFS layers 'MIDAS - udokumentowane złoża kopalin' (layer deposits: documented deposits) and
   'MIDAS - złoża wybilansowane (wyselekcjonowane dane archiwalne)' (layer deposits_removed: deposits removed from the
   balance). Grain: layer x objectid. A deposit (deposit_number, "Numer złoża") can have several contours: one per
   field ("Pole złoża") and per mineral (Kod, Kopalina; minerals_all lists every mineral of the deposit), so the
   deposit number is not unique here (Dev probe 45818533713: 14,955 numbers on 18,186 contours). Document, location
   method, coordinate system, mining office and the GIS area of the contour as published; the contour's centroid and a
   point on its surface (degrees, EPSG:4326); the geometry stays in Bronze. Text 'null' and empty text are NULL. #}
with newest as (
    select *
    from {{ source('pig_midas', 'pig_midas_features') }}
    where entity in ('deposits', 'deposits_removed')
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity as layer,
    {{ poc_pig_midas_integer('objectid', 'pig_midas_deposits') }} as objectid,
    {{ poc_pig_midas_integer('numer_zloza', 'pig_midas_deposits') }} as deposit_number,
    {{ poc_pig_midas_text('nazwa_zloza') }} as deposit_name,
    {{ poc_pig_midas_text('pole_zloza') }} as deposit_field,
    {{ poc_pig_midas_text('kod') }} as mineral_code,
    {{ poc_pig_midas_text('kopalina') }} as mineral,
    {{ poc_pig_midas_text('commodity') }} as mineral_en,
    {{ poc_pig_midas_text('wszystkie_kopaliny') }} as minerals_all,
    {{ poc_pig_midas_text('numer_dokumentu') }} as document_number,
    {{ poc_pig_midas_text('aktualizacja_granic') }} as boundary_update,
    {{ poc_pig_midas_text('wykreslenie_z_bilansu') }} as removal_note,
    {{ poc_pig_midas_text('uwagi') }} as remarks,
    {{ poc_pig_midas_text('w_granicach_innego_zloza') }} as within_other_deposit,
    {{ poc_pig_midas_text('lokalizacja') }} as location_method,
    {{ poc_pig_midas_text('lokalizacja_zalacznik') }} as location_annex,
    {{ poc_pig_midas_text('uklad_wspolrzednych') }} as coordinate_system,
    {{ poc_pig_midas_text('nadzor_gorniczy') }} as mining_office,
    cast({{ poc_pig_midas_text('shape_area_m2') }} as double) as shape_area_m2,
    {{ poc_pig_midas_points("nullif(geometry_geojson, 'null')") }},
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as snapshot_date,
    source_version,
    raw_sha256
from newest
