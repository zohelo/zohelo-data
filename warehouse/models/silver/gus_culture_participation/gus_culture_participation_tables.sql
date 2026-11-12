{# GUS "Uczestnictwo ludności w kulturze w <year> r." (register PLS-028): survey estimates (% of persons aged 15+)
   for Poland, total and by sex, reference year = the survey edition, from the newest copy of each edition
   (bronze.gus_culture_participation_cells). Grain: table_code x measure_code x group_code x period_year.
   Tables are found by title and column header (macro gus_table_values, warehouse/macros/gus_tables.sql); rows
   OGÓŁEM (T), Mężczyźni (M), Kobiety (F):
   - museums: "Zwiedzanie muzeów i galerii sztuki", column Zwiedzanie muzeów | w kraju | zwiedzający | razem
   - library: "Korzystanie z biblioteki lub czytelni" (2019: "... i czytelni"), column Korzystanie z biblioteki |
     korzystający | razem
   - theatre: "Uczestnictwo w wydarzeniach muzycznych i teatralnych" (2019: "Uczęszczanie do teatrów i instytucji
     muzycznych"), column Uczęszczanie do teatru (2019: na przedstawienia teatralne) | uczęszczający | razem
   Estimates marked "v" (low precision) carry status low_reliability; "." (not shown) has no value. #}
{% set sexes = [['T', '^OGÓŁEM$'], ['M', '^Mężczyźni$'], ['F', '^Kobiety$']] %}
{% set specs = [
    {'table': 'museums', 'title': 'Zwiedzanie muzeów i galerii sztuki', 'measure': 'museum_visiting',
     'header': '(^|\\| )Zwiedzanie muzeów.*\\| w kraju.*\\| zwiedzający.*\\| razem', 'unit': 'pct_population',
     'groups': sexes},
    {'table': 'library', 'title': 'Korzystanie z biblioteki (i|lub) czytelni', 'measure': 'library_use',
     'header': '(^|\\| )Korzystanie z biblio.*\\| korzystający.*\\| razem', 'unit': 'pct_population',
     'groups': sexes},
    {'table': 'theatre',
     'title': 'Uczestnictwo w wydarzeniach muzycznych i teatralnych|Uczęszczanie do teatrów i instytucji muzycznych',
     'measure': 'theatre_attendance', 'unit': 'pct_population',
     'header': '(^|\\| )Uczęszczanie (do teatru|na przedstawienia teatralne).*\\| uczęszczający.*\\| razem',
     'groups': sexes},
] %}
select * from ({{ gus_table_values(source('gus_culture_participation', 'gus_culture_participation_cells'), specs) }})
