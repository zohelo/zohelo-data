-- Silver: OpenData.org organizations (BrightQuery, Senzing COMPANY records).
-- Applied by the loader to ONE Bronze file (one member of the OpenData snapshot); `bronze` is that
-- file (design #875). Senzing features are a JSON array of objects; each value is the first object
-- that carries the key. Other features stay in bronze.opendata_records.features.
select
    entity,
    record_id,
    list_filter(json_extract_string(features, '$[*].NAME_ORG'), lambda v: v is not null)[1] as organization_name,
    list_filter(json_extract_string(features, '$[*].RECORD_TYPE'), lambda v: v is not null)[1] as record_type,
    list_filter(json_extract_string(features, '$[*].ADDR_LINE1'), lambda v: v is not null)[1] as address_line1,
    list_filter(json_extract_string(features, '$[*].ADDR_CITY'), lambda v: v is not null)[1] as address_city,
    list_filter(json_extract_string(features, '$[*].ADDR_STATE'), lambda v: v is not null)[1] as address_state,
    list_filter(json_extract_string(features, '$[*].ADDR_POSTAL_CODE'), lambda v: v is not null)[1] as address_postal_code,
    list_filter(json_extract_string(features, '$[*].ADDR_COUNTRY'), lambda v: v is not null)[1] as address_country,
    try_cast(list_filter(json_extract_string(features, '$[*].GEO_LATITUDE'), lambda v: v is not null)[1] as double) as latitude,
    try_cast(list_filter(json_extract_string(features, '$[*].GEO_LONGITUDE'), lambda v: v is not null)[1] as double) as longitude,
    list_filter(json_extract_string(features, '$[*].BQ_ID'), lambda v: v is not null)[1] as bq_id,
    list_filter(json_extract_string(features, '$[*].PLACEKEY'), lambda v: v is not null)[1] as placekey,
    data_source,
    raw_sha256,
    source_version
from bronze
