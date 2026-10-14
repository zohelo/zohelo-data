{# WDI economies (countries and aggregates) from the country member of the newest archive,
   one row per economy code with the World Bank's classification attributes. #}
with newest as (
    select * from {{ source('world_bank_wdi', 'wdi_bulk_metadata') }}
    where source_version = (select max(source_version) from {{ source('world_bank_wdi', 'wdi_bulk_metadata') }})
      and lower(member) like '%country.csv' and lower(member) not like '%series%'
),
records as (
    select source_row_number,
           max(case when field = 'Country Code' then value_text end) as economy_code,
           max(case when field = 'Short Name' then value_text end) as short_name,
           max(case when field = 'Table Name' then value_text end) as table_name,
           max(case when field = 'Long Name' then value_text end) as long_name,
           max(case when field = '2-alpha code' then value_text end) as iso2_code,
           max(case when field = 'WB-2 code' then value_text end) as wb2_code,
           max(case when field = 'Currency Unit' then value_text end) as currency_unit,
           max(case when field = 'Region' then value_text end) as region,
           max(case when field = 'Income Group' then value_text end) as income_group,
           max(case when field = 'Lending category' then value_text end) as lending_category,
           max(raw_sha256) as raw_sha256
    from newest
    group by source_row_number
)
select * exclude (source_row_number), region is null as is_aggregate
from records
where economy_code is not null
