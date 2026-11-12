{# GIOŚ groundwater quality monitoring (register PL-ENV-012): quality class per indicator of the nitrate study
   ("Klasy jakości wód podziemnych wg stężeń POSZCZEGÓLNYCH WSKAŹNIKÓW", 2025-), newest version per workbook. Grain:
   entity x row_number x column_number (one sample x one indicator); the sample is gios_groundwater_samples (entity,
   row_number), the indicator the column's header as published. #}
with cells as (
    {{ gios_groundwater_newest(['nitrate_classes']) }}
),

headers as (
    select
        entity,
        column_number,
        regexp_replace(trim(value_text), '\s+', ' ', 'g') as parameter
    from cells
    where row_number = 1
)

select
    c.entity,
    {{ gios_groundwater_year('c.entity') }} as year,
    c.row_number,
    c.column_number,
    h.parameter,
    trim(c.value_text) as quality_class,
    c.source_version,
    c.raw_sha256
from cells as c
inner join headers as h on c.entity = h.entity and c.column_number = h.column_number
where c.row_number > 1 and c.column_role = 'parameter_class'
