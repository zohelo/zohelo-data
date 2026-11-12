{# UN M49 numeric codes (https://unstats.un.org/unsd/methodology/m49/) of the EU27 member states and their ISO
   3166-1 alpha-2 code: the comparison countries of the international sources that key areas by M49 (FAOSTAT,
   WHO data.who.int). A code correspondence of the UN standard, not a name match. #}
{% macro poc_m49_eu27() %}
    select * from (
        values
        ('040', 'AT'), ('056', 'BE'), ('100', 'BG'), ('196', 'CY'), ('203', 'CZ'), ('276', 'DE'), ('208', 'DK'),
        ('233', 'EE'), ('724', 'ES'), ('246', 'FI'), ('250', 'FR'), ('300', 'GR'), ('191', 'HR'), ('348', 'HU'),
        ('372', 'IE'), ('380', 'IT'), ('440', 'LT'), ('442', 'LU'), ('428', 'LV'), ('470', 'MT'), ('528', 'NL'),
        ('616', 'PL'), ('620', 'PT'), ('642', 'RO'), ('752', 'SE'), ('705', 'SI'), ('703', 'SK')
    ) as t (m49, iso2)
{% endmacro %}
