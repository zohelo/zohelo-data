{# GUS "Turystyka w <year> r." (register PLS-035): curated tables of each edition, Poland and the 16 voivodeships,
   reference year = the edition year, from the newest copy of each edition (bronze.gus_tourism_cells).
   Grain: table_code x measure_code x area_name x period_year.
   Tables are found by title and column header (macro gus_table_values, warehouse/macros/gus_tables.sql):
   - establishments: TABL. I/1 "Turystyczne obiekty noclegowe" (31 July), column Ogółem
   - bed_places: TABL. I/2 "Miejsca noclegowe w turystycznych obiektach noclegowych" (31 July), column Ogółem
   - tourists: TABL. II/17 "Turyści korzystający z turystycznych obiektów noclegowych" by voivodeship, column Ogółem
   - overnight_stays: TABL. II/19 "Noclegi udzielone turystom" by voivodeship, column Ogółem
   Values as published; GUS symbols per the macro. #}
{% set specs = [
    {'table': 'establishments', 'title': 'I/1\\. ?TURYSTYCZNE OBIEKTY NOCLEGOWE', 'measure': 'establishments',
     'header': '(^|\\| )Ogółem', 'unit': 'facilities'},
    {'table': 'bed_places', 'title': 'I/2\\. ?MIEJSCA NOCLEGOWE', 'measure': 'bed_places',
     'header': '(^|\\| )Ogółem', 'unit': 'bed_places'},
    {'table': 'tourists', 'title': 'II/17\\. ?TURYŚCI KORZYSTAJĄCY', 'measure': 'tourists',
     'header': '(^|\\| )Ogółem', 'unit': 'persons'},
    {'table': 'overnight_stays', 'title': 'II/19\\. ?NOCLEGI UDZIELONE', 'measure': 'overnight_stays',
     'header': '(^|\\| )Ogółem', 'unit': 'overnight_stays'},
] %}
select * from ({{ gus_table_values(source('gus_tourism', 'gus_tourism_cells'), specs) }})
