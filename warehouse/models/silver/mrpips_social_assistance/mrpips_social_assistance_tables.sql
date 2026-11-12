{# MRPiPS social assistance statistics, annual MRPiPS-03 report (register PLS-031): DZIAŁ 2 "Rzeczywista liczba
   rodzin i osób objętych pomocą społeczną" by voivodeship (DZIAŁ 3 until 2020), row 1 (benefits granted as
   commissioned and own tasks of the municipalities): persons granted a benefit by decision, families, persons in
   those families; Poland and the 16 voivodeships, reference year = the report year, from the newest copy of each
   year's workbook (bronze.mrpips_social_assistance_cells). Grain: table_code x measure_code x area_name x group_code
   x period_year. Found by the column header (macro gus_table_values, warehouse/macros/gus_tables.sql): only the
   voivodeship sheet of that section has the header and voivodeship rows; the total row "RAZEM" is Poland. Values as
   published (persons, families). #}
{% set row1 = '(^|\\| )Świadczenia przyznane w ramach zadań zleconych i (zadań )?własnych.*\\| ' %}
{% set specs = [
    {'table': 'dzial_2', 'title': '.', 'measure': 'persons_granted', 'unit': 'persons',
     'header': row1 ~ 'Liczba osób, którym przyznano'},
    {'table': 'dzial_2', 'title': '.', 'measure': 'families', 'unit': 'families',
     'header': row1 ~ 'Liczba rodzin - ogółem'},
    {'table': 'dzial_2', 'title': '.', 'measure': 'persons_in_families', 'unit': 'persons',
     'header': row1 ~ 'Liczba osób w rodzinach'},
] %}
select * from (
    {{ gus_table_values(
        source('mrpips_social_assistance', 'mrpips_social_assistance_cells'),
        specs,
        poland_label='^(polska|razem)$'
    ) }}
)
