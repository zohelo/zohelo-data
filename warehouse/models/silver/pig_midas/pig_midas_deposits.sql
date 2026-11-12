{# PIG-PIB MIDAS mineral deposits (WFS, bronze.pig_midas_features): one row per deposit polygon of the newest
   download of the documented-deposits layer (layer 'documented') and of the selected archival layer of deposits
   removed from the balance (layer 'removed'). Grain: layer × object_id (the layer's own feature id); a deposit
   (deposit_number, the MIDAS deposit number) can have several polygons (fields A, B, … or separate contours), so
   counts of deposits are distinct deposit numbers. Text as published; a value that is only blanks is null; areas
   in m² (Shape_Area of the polygon in EPSG:2180, as computed by the server). No region code in the layer. #}
with newest as (
    select *
    from {{ source('pig_midas', 'pig_midas_features') }}
    where entity in ('documented_deposits', 'removed_deposits')
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    case entity when 'documented_deposits' then 'documented' else 'removed' end as layer,
    cast(object_id as bigint) as object_id,
    cast(deposit_number as bigint) as deposit_number,
    nullif(trim(deposit_name), '') as deposit_name,
    nullif(trim(deposit_field), '') as deposit_field,
    nullif(trim(mineral_code), '') as mineral_code,
    nullif(trim(mineral_name), '') as mineral_name,
    nullif(trim(commodity_en), '') as commodity_en,
    nullif(trim(all_minerals), '') as all_minerals,
    nullif(trim(document_number), '') as document_number,
    nullif(trim(mining_office), '') as mining_office,
    nullif(trim(location_method), '') as location_method,
    nullif(trim(location_annex), '') as location_annex,
    nullif(trim(coordinate_system), '') as coordinate_system,
    nullif(trim(boundary_update_note), '') as boundary_update_note,
    nullif(trim(other_deposit_note), '') as other_deposit_note,
    nullif(trim(removal_note), '') as removal_note,
    nullif(trim(remarks), '') as remarks,
    cast(shape_area_m2 as double) as shape_area_m2,
    geometry_type,
    feature_id,
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as snapshot_date,
    source_version,
    ingested_at,
    raw_sha256
from newest
