{# WHO data.who.int indicators (one CSV per indicator, entity life_expectancy = 90E2E48 WHOSIS_000001, hale =
   C64284D WHOSIS_000002): one row per area, year and sex of the newest Landing copy of each entity, typed. Areas by
   UN M49 code as published (unpadded: 616 Poland, 1 World) with their type (COUNTRY, WHOREGION,
   WORLDBANKINCOMEGROUP, GLOBAL); sex FEMALE, MALE or the total as published; value with its lower and upper bound
   (years). Only years (DIM_TIME_TYPE YEAR) are in these files; a duplicated row fails the unique test. #}
with newest as (
    select *
    from {{ source('who', 'who_observations') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    ind_code as indicator_code,
    ind_uuid as indicator_uid,
    ind_name as indicator_name,
    dim_geo_code_m49 as geo_code_m49,
    dim_geo_code_type as geo_type,
    geo_name_short as geo_name,
    dim_time_type as time_type,
    try_cast(dim_time as integer) as year,
    dim_sex as sex,
    try_cast(nullif(amount_n, '') as double) as value,
    try_cast(nullif(amount_nl, '') as double) as value_low,
    try_cast(nullif(amount_nu, '') as double) as value_high,
    dim_publish_state_code as publish_state,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from newest
