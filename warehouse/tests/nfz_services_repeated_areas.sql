{{ config(severity='warn') }}
-- NFZ services: tables whose basic-data answer gives one area (Poland or an NFZ branch) more than once. Silver keeps
-- every such record (area_records > 1) and does not choose between them; Gold leaves them out. Listed as a warning so
-- each build shows what is left out; returns entity, table, area and the records.
select
    entity,
    table_id,
    year,
    area_code,
    area_records,
    source_row_number,
    number_of_patients,
    number_of_hospitalizations
from {{ ref('nfz_services_basic_data') }}
where area_records > 1
