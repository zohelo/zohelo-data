{# GIOŚ groundwater quality monitoring (register PL-ENV-012): inorganic indicator results from 2008 (one result page a
   year) and the nitrate study (2025-), newest version per workbook. Grain: entity x row_number x column_number (one
   sample x one indicator); the sample is gios_groundwater_samples (entity, row_number). Values as published: value
   only for a number, markers ("<0,05", "n.o.") kept in value_text with value_flag, never 0. #}
{{ gios_groundwater_results(['inorganic', 'nitrate_inorganic'], 2008, 2100) }}
