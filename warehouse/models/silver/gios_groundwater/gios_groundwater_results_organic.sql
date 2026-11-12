{# GIOŚ groundwater quality monitoring (register PL-ENV-012): organic indicator results (2009-), newest version per
   workbook. Grain: entity x row_number x column_number (one sample x one indicator); the sample is
   gios_groundwater_samples (entity, row_number). Values as published: value only for a number, markers ("<0,00002",
   "n.o.") kept in value_text with value_flag, never 0. #}
{{ gios_groundwater_results(['organic'], 1991, 2100) }}
