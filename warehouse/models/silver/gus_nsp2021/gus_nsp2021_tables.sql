{# GUS Narodowy Spis Powszechny Ludności i Mieszkań 2021 (register PLS-005): curated final-results tables, Poland and
   the 16 voivodeships, from the newest copy of each declared workbook (bronze.gus_nsp2021_cells).
   Grain: table_code x measure_code x area_name x group_code x period_year (census reference date 31 March 2021; the
   2011 census values printed beside them have period_year 2011).
   Tables are found by title and column header (macro gus_table_values, warehouse/macros/gus_tables.sql):
   - population: TABL. 2 "Ludność według województw - dynamika zmian": Ogółem | 2011 and | 2021 (population),
     Miasta | 2021 (population_urban), Wieś | 2021 (population_rural), Ludność miejska w % | 2021 (urban_share)
   - labour: TABL. 1 "Aktywność ekonomiczna ludności w wieku 15 lat lub więcej" by voivodeship (sheet "TABL. 1.";
     tables 1, 2, 5 and 6 of the workbook share the title), block Ogółem: activity rate, employment rate and
     unemployment rate in % (census definitions, persons aged 15+)
   Values as published. #}
{% set specs = [
    {'table': 'population', 'title': 'LUDNOŚĆ WEDŁUG WOJEWÓDZTW - DYNAMIKA ZMIAN', 'measure': 'population',
     'header': '(^|\\| )Ogółem Total \\| 2021$', 'unit': 'persons', 'period': 2021},
    {'table': 'population', 'title': 'LUDNOŚĆ WEDŁUG WOJEWÓDZTW - DYNAMIKA ZMIAN', 'measure': 'population',
     'header': '(^|\\| )Ogółem Total \\| 2011$', 'unit': 'persons', 'period': 2011},
    {'table': 'population', 'title': 'LUDNOŚĆ WEDŁUG WOJEWÓDZTW - DYNAMIKA ZMIAN', 'measure': 'population_urban',
     'header': '(^|\\| )Miasta Urban areas \\| 2021$', 'unit': 'persons', 'period': 2021},
    {'table': 'population', 'title': 'LUDNOŚĆ WEDŁUG WOJEWÓDZTW - DYNAMIKA ZMIAN', 'measure': 'population_rural',
     'header': '(^|\\| )Wieś Rural areas \\| 2021$', 'unit': 'persons', 'period': 2021},
    {'table': 'population', 'title': 'LUDNOŚĆ WEDŁUG WOJEWÓDZTW - DYNAMIKA ZMIAN', 'measure': 'urban_share',
     'header': '(^|\\| )Ludność miejska w % .*\\| 2021$', 'unit': 'pct_population', 'period': 2021},
    {'table': 'labour', 'title': 'AKTYWNOŚĆ EKONOMICZNA LUDNOŚCI W WIEKU 15 LAT', 'measure': 'activity_rate',
     'header': '(^|\\| )Współ-?czynnik aktywności', 'unit': 'pct', 'period': 2021, 'block': '^Ogółem',
     'sheet': '^TABL\\. 1\\.$'},
    {'table': 'labour', 'title': 'AKTYWNOŚĆ EKONOMICZNA LUDNOŚCI W WIEKU 15 LAT', 'measure': 'employment_rate',
     'header': '(^|\\| )Wskaźnik zatrudnienia', 'unit': 'pct', 'period': 2021, 'block': '^Ogółem',
     'sheet': '^TABL\\. 1\\.$'},
    {'table': 'labour', 'title': 'AKTYWNOŚĆ EKONOMICZNA LUDNOŚCI W WIEKU 15 LAT', 'measure': 'unemployment_rate',
     'header': '(^|\\| )Stopa bezrobocia', 'unit': 'pct', 'period': 2021, 'block': '^Ogółem',
     'sheet': '^TABL\\. 1\\.$'},
] %}
select * from ({{ gus_table_values(source('gus_nsp2021', 'gus_nsp2021_cells'), specs) }})
