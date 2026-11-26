-- Silver: GUGiK elevation sheet indexes (WFS "Skorowidze" of NMT, NMPT and the LIDAR measurement data, EVRF2007 and
-- KRON86 services), typed. Applied by the loader to ONE Bronze file, the features of one service's year (design #875,
-- ingestion/to_bronze/gugik_nmt/); `bronze` is that file. Grain: one row per index feature = sheet x product x
-- edition x file format (entity, feature_id: the number in the feature's gml:id; one sheet id can hold several
-- editions and file formats in a year).
-- product (nmt terrain model, nmpt surface model, lidar point cloud) and the service's vertical datum come from the
-- entity; NMT and NMPT are different surfaces and never summed. Values as published, typed; no raster or point cloud.
-- The footprint is the published polygon (EPSG:2180; GML 3.2 with the urn EPSG code lists northing before easting, so
-- x = the second number of each pair): its centroid (area-weighted, shoelace formula, relative to the first vertex) and
-- its planar area in km2 are computed for a one-ring Polygon; other geometries keep them null.
with points as (
    select
        *,
        list_transform(regexp_split_to_array(trim(pos_list), '\s+'), lambda v: try_cast(v as double)) as c
    from bronze
),

rings as (
    select
        *,
        list_transform(
            range(0, len(c) // 2 - 1),
            lambda i: (c[2 * i + 2] - c[2]) * (c[2 * i + 3] - c[1]) - (c[2 * i + 4] - c[2]) * (c[2 * i + 1] - c[1])
        ) as cross_terms,
        list_transform(range(0, len(c) // 2 - 1), lambda i: c[2 * i + 2] + c[2 * i + 4] - 2 * c[2]) as x_terms,
        list_transform(range(0, len(c) // 2 - 1), lambda i: c[2 * i + 1] + c[2 * i + 3] - 2 * c[1]) as y_terms
    from points
),

shapes as (
    select
        *,
        geometry_type = 'Polygon' and rings = '1' and len(c) >= 8 and len(c) % 2 = 0
        and list_bool_and(list_transform(c, lambda v: v is not null))
        and c[1] = c[len(c) - 1] and c[2] = c[len(c)] as simple_ring,
        list_sum(cross_terms) as twice_area
    from rings
)

select
    entity,
    feature_type,
    split_part(entity, '_', 1) as product,
    split_part(entity, '_', 2) as service_datum,
    try_cast(split_part(gml_id, '.', 2) as bigint) as feature_id,
    trim(godlo) as sheet_id,
    try_cast(akt_rok as integer) as data_year,
    try_cast(akt_data as date) as acquisition_date,
    asortyment as assortment,
    format as file_format,
    char_przestrz as spatial_characteristic,
    try_cast(blad_sr_wys as double) as height_error_m,
    try_cast(blad_sr_syt as double) as position_error_m,
    uklad_xy as crs_xy,
    uklad_h as vertical_datum,
    modul_archiwizacji as archive_scale,
    nr_zglosz as submission_no,
    czy_ark_wypelniony as sheet_filled,
    url_do_pobrania as download_url,
    zrodlo_danych as data_source,
    try_cast(dt_pzgik as date) as pzgik_date,
    geometry_type,
    srs_name,
    case when simple_ring and twice_area <> 0
        then round(c[2] + list_sum(list_transform(range(1, len(cross_terms) + 1),
            lambda k: x_terms[k] * cross_terms[k])) / (3 * twice_area), 2)
    end as centroid_x_2180,
    case when simple_ring and twice_area <> 0
        then round(c[1] + list_sum(list_transform(range(1, len(cross_terms) + 1),
            lambda k: y_terms[k] * cross_terms[k])) / (3 * twice_area), 2)
    end as centroid_y_2180,
    case when simple_ring then round(abs(twice_area) / 2 / 1e6, 6) end as footprint_area_km2,
    raw_sha256,
    source_version
from shapes
