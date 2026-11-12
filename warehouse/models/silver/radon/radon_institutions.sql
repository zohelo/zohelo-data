{# RAD-on (OPI PIB) institutions of the higher education and science system from POL-on (register PLS-023): one row
   per institution record of the newest Landing copy (bronze.radon_institutions). Grain: institution_uuid (POL-on's
   own id). Every kind POL-on lists (universities, research institutes, PAN institutes, other institutions) and every
   status (operating, in liquidation, liquidated), codes and names as published. Non-person fields only: the head
   of the institution and contact details are dropped before Landing. Dates typed from ISO text; voivodeship name in
   lower case as published (null for institutions abroad or without one). #}
with newest as (
    select *
    from {{ source('radon', 'radon_institutions') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    institutionuuid as institution_uuid,
    institutionuid as institution_uid,
    id as polon_id,
    name,
    regon,
    nip,
    krs,
    ikindcd as kind_code,
    ikindname as kind_name,
    utypecd as university_type_code,
    utypename as university_type_name,
    sitypecd as science_institution_type_code,
    sitypename as science_institution_type_name,
    statuscode as status_code,
    status as status_name,
    try_cast(istartdt as date) as start_date,
    try_cast(iliqstartdt as date) as liquidation_start_date,
    try_cast(iliqdt as date) as liquidation_date,
    voivodeshipcode as voivodeship_code,
    lower(voivodeship) as voivodeship,
    city,
    postalcd as postal_code,
    country,
    countrycd as country_code,
    www,
    supervisinginstitutionid as supervising_institution_uuid,
    supervisinginstitutionname as supervising_institution_name,
    ministrynumber as ministry_number,
    eunnumber as eun_number,
    pannumber as pan_number,
    pib,
    yearpib as pib_year,
    federationnumber as federation_number,
    datasource as data_source,
    lastrefresh as last_refresh,
    source_version,
    raw_sha256
from newest
