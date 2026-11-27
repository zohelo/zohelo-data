{# WIPO IP Statistics (register GL-032): industrial design counts (the number of designs, not of applications) in
   applications and in registrations, by office, applicant origin, Locarno class (as published, 0 to 32) and year, as
   published in dc_indicator_design_3_application_by_class.csv and dc_indicator_design_4_registration_by_class.csv of
   the newest copy of entity design_indicators (bronze.wipo_ip_stats_counts). The files hold the offices that report
   designs by class, so an office's yearly sum here can be far below its own design count (in the 2024 file the
   Polish office has at most 61 designs a year in applications before 2018, Dev probe 45872152431). Codes as published (ISO 3166-1 alpha-2, regional offices
   EM, BX, OA, AP, EA, historical AN, origin '**' not stated); value is the published count (integers of at least 1);
   a count or year of another shape gives null and fails the not_null tests. Grain: measure x office_code x
   origin_code x locarno_class x year. #}
with records as (
    select *
    from {{ source('wipo_ip_stats', 'wipo_ip_stats_counts') }}
    where entity = 'design_indicators'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    indicator,
    case indicator
        when 'design_3_application_by_class' then 'applications'
        when 'design_4_registration_by_class' then 'registrations'
    end as measure,
    case when regexp_full_match(year, '(19|20)[0-9]{2}') then cast(year as integer) end as year,
    office as office_code,
    origin as origin_code,
    class_id as locarno_class,
    case when regexp_full_match(count, '[0-9]+') then cast(count as bigint) end as value,
    raw_sha256,
    source_version
from records
