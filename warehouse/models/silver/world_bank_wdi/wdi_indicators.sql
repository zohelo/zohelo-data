{# WDI indicators (series) from the series member of the newest archive: code, name,
   topic, unit, periodicity, definitions, source and licence. #}
with newest as (
    select * from {{ source('world_bank_wdi', 'wdi_bulk_metadata') }}
    where source_version = (select max(source_version) from {{ source('world_bank_wdi', 'wdi_bulk_metadata') }})
      and lower(member) like '%series.csv' and lower(member) not like '%country%'
)
select
    max(case when field = 'Series Code' then value_text end) as indicator_code,
    max(case when field = 'Indicator Name' then value_text end) as indicator_name,
    max(case when field = 'Topic' then value_text end) as topic,
    max(case when field = 'Unit of measure' then value_text end) as unit_of_measure,
    max(case when field = 'Periodicity' then value_text end) as periodicity,
    max(case when field = 'Short definition' then value_text end) as short_definition,
    max(case when field = 'Long definition' then value_text end) as long_definition,
    max(case when field = 'Source' then value_text end) as source_note,
    max(case when field = 'License Type' then value_text end) as license_type,
    max(raw_sha256) as raw_sha256
from newest
group by source_row_number
having max(case when field = 'Series Code' then value_text end) is not null
