-- GUGiK PRG address points (register PL-ENV-007, #3106): one row per address point of the newest national address
-- package (layer PRG_PunktyAdresowe_POLSKA), grain address_id (ID_IIP, the IIP identifier of the point: namespace,
-- local id and version stamp; the stamp is also address_version_at, the local id is not stored twice), typed. Source
-- codes kept as published: TERC of the gmina (TERYT_GMI), SIMC of the locality, ULIC of the street (null for an
-- address without a street; a few published values are not 5 digits and stay as printed); the postal code only when
-- it reads NN-NNN (the printed text stays in postal_code_published; 751 points print the text 'null'). Dates assigned
-- as published (a few read 0202 or 2209: typos of the registers).
-- Coordinates as published, EPSG:2180 (ETRF2000-PL / CS92, metres; x = easting, y = northing); not reprojected.
-- No persons: the layer holds addresses, not residents. Applied by ingestion/to_bronze/gugik_prg/entities.py to ONE
-- Bronze file (`bronze`); the loader rejects the package when address_id repeats or a key is null (design #875).
select
    entity,
    id_iip as address_id,
    try_cast(
        regexp_extract(id_iip, '_([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]{8}[+-][0-9]{2}:[0-9]{2})$', 1) as timestamptz
    ) as address_version_at,
    teryt_gmi as gmina_code,
    nazwa_gmi as gmina_name,
    id_simc as simc_code,
    nazwa_msc as locality_name,
    id_ulic as ulic_code,
    nazwa_ulc as street_name,
    numer_porz as house_number,
    case when regexp_full_match(kod_poczt, '[0-9]{2}-[0-9]{3}') then kod_poczt end as postal_code,
    kod_poczt as postal_code_published,
    cast(data_nad as date) as assigned_on,
    cast(x as double) as x_epsg2180,
    cast(y as double) as y_epsg2180,
    cast(ogc_fid as bigint) as feature_number,
    source_version,
    raw_sha256,
    ingested_at
from bronze
