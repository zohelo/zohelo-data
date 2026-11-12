{# GUS "Zdrowie i ochrona zdrowia w <year> r." (register PLS-004, health family): curated tables of each edition,
   Poland and the 16 voivodeships, reference year = the edition year, from the newest copy of each edition
   (bronze.gus_health_cells). Grain: table_code x measure_code x area_name x period_year.
   Tables are found by title and column header (macro gus_table_values, warehouse/macros/gus_tables.sql):
   - medical_personnel: "Pracownicy medyczni według województw" (not "... według wieku i województw" nor
     "... posiadający prawo wykonywania zawodu"), 31 December: doctors (column Lekarze | razem), nurses
     (Pielęgniarki | razem)
   - general_hospitals: "Szpitale ogólne według województw", hospitals (Szpitale, 31 December), beds (Łóżka |
     ogółem, 31 December), beds per 10 thousand population (Łóżka | na 10 tys. ludności), inpatients (Leczeni |
     ogółem, in the year)
   Values as published (persons, facilities, beds, beds per 10,000 population); GUS symbols per the macro. #}
{% set specs = [
    {'table': 'medical_personnel', 'title': 'Pracownicy medyczni według województw', 'measure': 'doctors',
     'header': '(^|\\| )Lekarze( Doctors)?( \\| razem|$)', 'unit': 'persons'},
    {'table': 'medical_personnel', 'title': 'Pracownicy medyczni według województw', 'measure': 'nurses',
     'header': '(^|\\| )Pielęgniarki( Nurses)?( \\| razem|$)', 'unit': 'persons'},
    {'table': 'general_hospitals', 'title': 'Szpitale ogólne[^ ]* według województw', 'measure': 'hospitals',
     'header': '(^|\\| )Szpitale( Hospitals)?( \\||$)', 'unit': 'facilities'},
    {'table': 'general_hospitals', 'title': 'Szpitale ogólne[^ ]* według województw', 'measure': 'beds',
     'header': '(^|\\| )Łóżka[^|]* \\| ogółem', 'unit': 'beds'},
    {'table': 'general_hospitals', 'title': 'Szpitale ogólne[^ ]* według województw', 'measure': 'beds_per_10k',
     'header': '(^|\\| )Łóżka[^|]* \\| na 10 tys', 'unit': 'per_10k_population'},
    {'table': 'general_hospitals', 'title': 'Szpitale ogólne[^ ]* według województw', 'measure': 'inpatients',
     'header': '(^|\\| )Leczeni[^|]* \\| ogółem', 'unit': 'persons'},
] %}
select * from ({{ gus_table_values(source('gus_health', 'gus_health_cells'), specs) }})
