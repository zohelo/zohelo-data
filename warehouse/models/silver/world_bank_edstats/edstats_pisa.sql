{# OECD PISA country mean scores as republished by the World Bank Education Statistics (API source 12,
   bronze.edstats_observations): one row per subject (entity) × indicator (both sexes, .FE female, .MA male) ×
   economy × assessment year of the newest Landing copy of each selection, typed. Scores as the OECD published them
   (PISA scale points); obs_status kept. A duplicated row fails the unique test. #}
with newest as (
    select *
    from {{ source('world_bank_edstats', 'edstats_observations') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    entity,
    indicator_id,
    indicator_name,
    case
        when indicator_id like '%.FE' then 'F'
        when indicator_id like '%.MA' then 'M'
        else 'T'
    end as sex_code,
    country_id,
    country_iso3,
    country_name,
    cast(year as integer) as year,
    cast(value_text as double) as value,
    obs_status,
    source_last_updated,
    raw_sha256,
    source_version,
    ingested_at,
    source_row_number
from newest
