{# IDS series from the series member of the newest archive: code, name, definitions, topic, source,
   aggregation method and licence. #}
with newest as (
    select * from {{ source('world_bank_ids', 'ids_metadata') }}
    where
        source_version
        = (select max(latest.source_version) from {{ source('world_bank_ids', 'ids_metadata') }} as latest)
        and lower(member) like '%seriesmetadata.csv' and lower(member) not like '%country%'
)

select
    trim(max(case when field = 'Code' then value_text end)) as series_code,
    max(case when field = 'Indicator Name' then value_text end) as series_name,
    max(case when field = 'Short definition' then value_text end) as short_definition,
    max(case when field = 'Long definition' then value_text end) as long_definition,
    max(case when field = 'Topic' then value_text end) as topic,
    max(case when field = 'Source' then value_text end) as source_note,
    max(case when field = 'Aggregation method' then value_text end) as aggregation_method,
    max(case when field = 'License Type' then value_text end) as license_type,
    max(raw_sha256) as raw_sha256
from newest
group by member, source_row_number
having max(case when field = 'Code' then value_text end) is not null
