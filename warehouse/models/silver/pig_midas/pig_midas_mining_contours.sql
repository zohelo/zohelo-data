{# PIG-PIB MIDAS mining areas (obszary górnicze) and mining terrains (tereny górnicze) (WFS,
   bronze.pig_midas_features): one row per contour of the newest download of the four layers, current (status
   'aktualny') and selected archival ones (status 'zniesiony', abolished). Grain: layer × contour_id (ID_konturu,
   the layer's own contour id). A mining area or terrain (register_number, its number in the register of mining
   areas) can have several contours. deposit_number links to the deposit of pig_midas_deposits (published as a
   decimal, e.g. 14619.00000000; cast to an integer only when it is whole). Dates are published as dd/mm/yyyy;
   valid_until is the end of validity the decision set; a value that is only blanks is null. Areas in m²: the
   declared area of the decision and the polygon's Shape_Area (EPSG:2180, as computed by the server).
   coordinate_system is the EPSG code of the coordinates in the source documents. No region code in the layers. #}
with newest as (
    select *
    from {{ source('pig_midas', 'pig_midas_features') }}
    where entity in ('mining_areas', 'mining_terrains', 'archival_mining_areas', 'archival_mining_terrains')
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

typed as (
    select
        *,
        try_cast(deposit_number as decimal(18, 6)) as deposit_number_dec
    from newest
)

select
    entity as layer,
    case when entity like '%mining_areas' then 'mining_area' else 'mining_terrain' end as kind,
    nullif(trim(status), '') as status,
    cast(contour_id as bigint) as contour_id,
    cast(object_id as bigint) as object_id,
    nullif(trim(register_number), '') as register_number,
    nullif(trim(area_name), '') as area_name,
    case when deposit_number_dec = round(deposit_number_dec) then cast(deposit_number_dec as bigint) end
        as deposit_number,
    deposit_number as deposit_number_text,
    nullif(trim(deposit_name), '') as deposit_name,
    nullif(trim(mineral_name), '') as mineral_name,
    cast(strptime(nullif(trim(established_on), ''), '%d/%m/%Y') as date) as established_on,
    cast(strptime(nullif(trim(valid_until), ''), '%d/%m/%Y') as date) as valid_until,
    cast(strptime(nullif(trim(abolished_on), ''), '%d/%m/%Y') as date) as abolished_on,
    nullif(trim(establishing_decision), '') as establishing_decision,
    nullif(trim(establishing_authority), '') as establishing_authority,
    nullif(trim(abolishing_decision), '') as abolishing_decision,
    nullif(trim(abolishing_authority), '') as abolishing_authority,
    nullif(trim(mining_office), '') as mining_office,
    nullif(trim(map_sheet), '') as map_sheet,
    cast(nullif(trim(coordinate_system), '') as integer) as coordinate_system_epsg,
    nullif(trim(remarks), '') as remarks,
    cast(declared_area_m2 as double) as declared_area_m2,
    cast(shape_area_m2 as double) as shape_area_m2,
    geometry_type,
    feature_id,
    cast(strptime(source_version, '%Y%m%dT%H%M%SZ') as date) as snapshot_date,
    source_version,
    ingested_at,
    raw_sha256
from typed
