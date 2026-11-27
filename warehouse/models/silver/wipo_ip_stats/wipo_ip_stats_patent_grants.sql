{# WIPO IP Statistics (register GL-032): patent grants by filing office, applicant origin, WIPO technology field (35
   fields and 0 'Unknown') and year, as published in dc_indicator_patent_5_grant_by_technology.csv of the newest copy of
   entity patent_indicators (bronze.wipo_ip_stats_counts). Codes, field names and values as in
   wipo_ip_stats_patent_publications (same file, same rules). Grain: office_code x origin_code x technology_field_code
   x year. #}
with records as (
    select *
    from {{ source('wipo_ip_stats', 'wipo_ip_stats_counts') }}
    where entity = 'patent_indicators'
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

fields as (
    select
        raw_sha256,
        class_id,
        class_label
    from records
    where indicator = 'technology_field_names'
)

select
    r.entity,
    r.indicator,
    'grants' as measure,
    case when regexp_full_match(r.year, '(19|20)[0-9]{2}') then cast(r.year as integer) end as year,
    r.office as office_code,
    r.origin as origin_code,
    r.class_id as technology_field_code,
    f.class_label as technology_field,
    case when regexp_full_match(r.count, '[0-9]+') then cast(r.count as bigint) end as value,
    r.raw_sha256,
    r.source_version
from records as r
left join fields as f on r.raw_sha256 = f.raw_sha256 and r.class_id = f.class_id
where r.indicator = 'patent_5_grant_by_technology'
