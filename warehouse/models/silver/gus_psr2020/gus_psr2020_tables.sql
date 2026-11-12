{# GUS Powszechny Spis Rolny 2020 (register PLS-006): curated tables of "Charakterystyka gospodarstw rolnych w
   2020 r." (part 2, by voivodeship), Poland and the 16 voivodeships, from the newest copy of the workbook
   (bronze.gus_psr2020_cells). Grain: table_code x measure_code x area_name x group_code x period_year (reference
   year 2020; the 2010 census values printed beside them have period_year 2010).
   Tables are found by title and column header (macro gus_table_values, warehouse/macros/gus_tables.sql):
   - farms: TABL. 13 "Gospodarstwa rolne w latach 2010 i 2020 według województw", block "w liczbach
     bezwzględnych", columns 2010 | ogółem and 2020 | ogółem (agricultural holdings)
   - land: TABL. 18 "Użytkowanie gruntów według województw", block Ogółem: Użytki rolne | razem (agricultural
     land, ha)
   - average_area: TABL. 20 "Średnia powierzchnia użytków rolnych w gospodarstwach", block Ogółem: Ogółem (ha per
     holding)
   - livestock: TABL. 24 "Pogłowie zwierząt gospodarskich według województw", block Ogółem: Bydło | ogółem and
     Świnie | ogółem (heads)
   Values as published. #}
{% set specs = [
    {'table': 'farms', 'title': 'TABL\\. 13\\. GOSPODARSTWA ROLNE W LATACH 2010 i 2020', 'measure': 'farms',
     'header': '(^|\\| )2020 \\| ogółem', 'unit': 'farms', 'period': 2020, 'block': 'w liczbach bezwzględnych'},
    {'table': 'farms', 'title': 'TABL\\. 13\\. GOSPODARSTWA ROLNE W LATACH 2010 i 2020', 'measure': 'farms',
     'header': '(^|\\| )2010 \\| ogółem', 'unit': 'farms', 'period': 2010, 'block': 'w liczbach bezwzględnych'},
    {'table': 'land', 'title': 'TABL\\. 18\\. UŻYTKOWANIE GRUNTÓW WEDŁUG WOJEWÓDZTW',
     'measure': 'agricultural_land',
     'header': '(^|\\| )Użytki rolne Agricultural land \\| razem', 'unit': 'ha', 'period': 2020, 'block': '^Ogółem'},
    {'table': 'average_area', 'title': 'TABL\\. 20\\. ŚREDNIA POWIERZCHNIA UŻYTKÓW ROLNYCH',
     'measure': 'average_agricultural_land', 'header': '(^|\\| )Ogółem Total( \\||$)', 'unit': 'ha', 'period': 2020,
     'block': '^Ogółem'},
    {'table': 'livestock', 'title': 'TABL\\. 24\\. POGŁOWIE ZWIERZĄT GOSPODARSKICH', 'measure': 'cattle',
     'header': '(^|\\| )Bydło Cattle \\| ogółem', 'unit': 'heads', 'period': 2020, 'block': '^Ogółem'},
    {'table': 'livestock', 'title': 'TABL\\. 24\\. POGŁOWIE ZWIERZĄT GOSPODARSKICH', 'measure': 'pigs',
     'header': '(^|\\| )Świnie Pigs \\| ogółem', 'unit': 'heads', 'period': 2020, 'block': '^Ogółem'},
] %}
select * from ({{ gus_table_values(source('gus_psr2020', 'gus_psr2020_cells'), specs) }})
