-- Trial model: proves dbt can read Bronze and write an Iceberg table to the R2 catalog.
-- Replace with the real typed/cleaned Silver model when TERYT is modelled.
select * from {{ source('gus_teryt', 'teryt_terc') }}
