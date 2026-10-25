-- Silver: OpenData.org people linked to businesses (Senzing PEOPLE_BUSINESS records).
-- Applied by the loader to ONE Bronze file (one member of the OpenData snapshot); `bronze` is that
-- file (design #875). Senzing features are a JSON array of objects; each value is the first object
-- that carries the key. Other features stay in bronze.opendata_records.features.
select
    entity,
    record_id,
    list_filter(json_extract_string(features, '$[*].NAME_FULL'), v -> v is not null)[1] as person_name,
    list_filter(json_extract_string(features, '$[*].NAME_FIRST'), v -> v is not null)[1] as first_name,
    list_filter(json_extract_string(features, '$[*].NAME_LAST'), v -> v is not null)[1] as last_name,
    list_filter(json_extract_string(features, '$[*].RECORD_TYPE'), v -> v is not null)[1] as record_type,
    list_filter(json_extract_string(features, '$[*].ADDR_COUNTRY'), v -> v is not null)[1] as address_country,
    list_filter(json_extract_string(features, '$[*].REL_POINTER_KEY'), v -> v is not null)[1] as organization_bq_id,
    list_filter(json_extract_string(features, '$[*].REL_POINTER_ROLE'), v -> v is not null)[1] as role,
    list_filter(json_extract_string(features, '$[*].LINKEDIN'), v -> v is not null)[1] as linkedin_url,
    data_source,
    raw_sha256,
    source_version
from bronze
