-- Silver: OpenData.org locations of organizations (Senzing LOCATION records).
-- Applied by the loader to ONE Bronze file (one member of the OpenData snapshot); `bronze` is that
-- file (design #875). Senzing features are a JSON array of objects; each value is the first object
-- that carries the key. Other features stay in bronze.opendata_records.features.
select
    entity,
    record_id,
    coalesce(list_filter(json_extract_string(features, '$[*].NAME_ORG'), v -> v is not null)[1], list_filter(json_extract_string(features, '$[*].NAME_FULL'), v -> v is not null)[1]) as location_name,
    list_filter(json_extract_string(features, '$[*].RECORD_TYPE'), v -> v is not null)[1] as record_type,
    list_filter(json_extract_string(features, '$[*].ADDR_LINE1'), v -> v is not null)[1] as address_line1,
    list_filter(json_extract_string(features, '$[*].ADDR_CITY'), v -> v is not null)[1] as address_city,
    list_filter(json_extract_string(features, '$[*].ADDR_STATE'), v -> v is not null)[1] as address_state,
    list_filter(json_extract_string(features, '$[*].ADDR_POSTAL_CODE'), v -> v is not null)[1] as address_postal_code,
    list_filter(json_extract_string(features, '$[*].ADDR_COUNTRY'), v -> v is not null)[1] as address_country,
    try_cast(list_filter(json_extract_string(features, '$[*].GEO_LATITUDE'), v -> v is not null)[1] as double) as latitude,
    try_cast(list_filter(json_extract_string(features, '$[*].GEO_LONGITUDE'), v -> v is not null)[1] as double) as longitude,
    list_filter(json_extract_string(features, '$[*].REL_POINTER_KEY'), v -> v is not null)[1] as organization_bq_id,
    list_filter(json_extract_string(features, '$[*].REL_POINTER_ROLE'), v -> v is not null)[1] as role,
    list_filter(json_extract_string(features, '$[*].PLACEKEY'), v -> v is not null)[1] as placekey,
    data_source,
    raw_sha256,
    source_version
from bronze
