{# GUS "Kultura i dziedzictwo narodowe w <year> r." (register PLS-027): curated tables of each edition, Poland and the
   16 voivodeships, reference year = the edition year, from the newest copy of each edition
   (bronze.gus_culture_cells). Grain: table_code x measure_code x area_name x period_year.
   Tables are found by title and column header (macro gus_table_values, warehouse/macros/gus_tables.sql):
   - museum_visitors: "Zwiedzający muzea i wystawy muzealne według rodzajów muzeów i województw", column Ogółem
   - cinema_admissions: "Widzowie w kinach" (not "... w kinach stałych"), column Ogółem [| ogółem]
   - cinemas: "Kina i miejsca w kinach stałych" (not the urban / rural tables), column Kina | ogółem (2025) or
     Ogółem (until 2024)
   - library_users: "Czytelnicy i wypożyczenia na zewnątrz w bibliotekach publicznych" (not the urban / rural
     tables), column Czytelnicy | ogółem
   - public_libraries: "Biblioteki publiczne" (with branches), column Biblioteki i filie | ogółem
   Values as published (persons, facilities); GUS symbols per the macro. #}
{% set specs = [
    {'table': 'museum_visitors', 'title': 'Zwiedzający muzea i wystawy muzealne według rodzajów muzeów i województw',
     'measure': 'museum_visitors', 'header': '(^|\\| )Ogółem', 'unit': 'persons'},
    {'table': 'cinema_admissions', 'title': 'Widzowie w kinach (?-i:[A-Z])', 'measure': 'cinema_admissions',
     'header': '(^|\\| )Ogółem Grand total( \\| ogółem grand total)?$', 'unit': 'persons'},
    {'table': 'cinemas', 'title': 'Kina i miejsca w kinach stałych (Stan|Cinemas|Powrót)', 'measure': 'cinemas',
     'header': '^Ogółem Grand total$|(^|\\| )Kina\\b[^|]*\\| ogółem', 'unit': 'facilities'},
    {'table': 'library_users',
     'title': 'Czytelnicy i wypożyczenia na zewnątrz w bibliotekach publicznych( w [0-9]{4} r\\.)? (?-i:[A-Z])',
     'measure': 'library_users', 'header': '(^|\\| )Czytelnicy\\b.*\\| ogółem', 'unit': 'persons'},
    {'table': 'public_libraries', 'title': '[0-9]\\)?\\.? ?Biblioteki publiczne( Stan| Powrót| Województwa|$)',
     'measure': 'public_libraries', 'header': '(^|\\| )Biblioteki i filie.*\\| ogółem', 'unit': 'facilities'},
] %}
select * from ({{ gus_table_values(source('gus_culture', 'gus_culture_cells'), specs) }})
