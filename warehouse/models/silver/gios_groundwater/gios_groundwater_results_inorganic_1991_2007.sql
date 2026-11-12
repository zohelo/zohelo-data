{# GIOŚ groundwater quality monitoring (register PL-ENV-012): inorganic indicator results 1991-2007 (the result pages
   "1991-2003" and "2004-2007"; no longer updated), newest version per workbook. Grain: entity x row_number x
   column_number (one sample x one indicator); the sample is gios_groundwater_samples (entity, row_number). Values as
   published: value only for a number, markers ("<0,05", "n.o.") kept in value_text with value_flag, never 0. Split
   from the 2008- results only to keep each full-rebuild table below 1 M rows (#822). #}
{{ gios_groundwater_results(['inorganic'], 1991, 2007) }}
