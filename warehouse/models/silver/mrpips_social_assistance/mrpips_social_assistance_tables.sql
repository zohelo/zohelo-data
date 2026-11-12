{# MRPiPS social assistance statistics, annual MRPiPS-03 report (register PLS-031): DZIAŁ 2 "Rzeczywista liczba
   rodzin i osób objętych pomocą społeczną" by voivodeship, row 1 (benefits granted as commissioned and own tasks
   of the municipalities): persons granted a benefit by decision, families, persons in those families; Poland and
   the 16 voivodeships, reference year = the report year, from the newest copy of each year's workbook
   (bronze.mrpips_social_assistance_cells). Grain: table_code x measure_code x area_name x group_code x period_year.
   Found by sheet ("DZIAŁ 2"), title and column header (macro gus_table_values, warehouse/macros/gus_tables.sql).
   Values as published (persons, families). #}
{% set specs = [
    {'table': 'dzial_2', 'title': '.', 'sheet': '^DZIAŁ 2$', 'measure': 'persons_granted', 'unit': 'persons',
     'header': '(^|\\| )Świadczenia przyznane w ramach zadań zleconych.*\\| Liczba osób, którym przyznano'},
    {'table': 'dzial_2', 'title': '.', 'sheet': '^DZIAŁ 2$', 'measure': 'families', 'unit': 'families',
     'header': '(^|\\| )Świadczenia przyznane w ramach zadań zleconych.*\\| Liczba rodzin - ogółem'},
    {'table': 'dzial_2', 'title': '.', 'sheet': '^DZIAŁ 2$', 'measure': 'persons_in_families', 'unit': 'persons',
     'header': '(^|\\| )Świadczenia przyznane w ramach zadań zleconych.*\\| Liczba osób w rodzinach'},
] %}
select * from ({{ gus_table_values(source('mrpips_social_assistance', 'mrpips_social_assistance_cells'), specs) }})
