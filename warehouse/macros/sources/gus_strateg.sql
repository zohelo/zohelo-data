{# gus_strateg (register PLS-003, #2430): Gold registration, decision D34 (docs/source-registration.md). Block 'sor'
   (models/gold/poc_build_gus_strateg.sql). #}
{% macro poc_source__gus_strateg() %}
    {{ return({'block_model': 'poc_build_gus_strateg', 'blocks': [['sor', 'GUS STRATEG: indicators of the Strategy for Responsible Development (SOR)']]}) }}
{% endmacro %}

{# STRATEG territory codes (12 digits; territorial-data answer, Dev probe 43800593902). Level 0: the EU countries by
   the first two digits (Polish names as published: 10 POLSKA ... 64 Włochy) → ISO 3166 alpha-2 through
   dim_geography; 05 'Unia Europejska - 27 (od 2020 r.)' → EUROSTAT:EU27_2020, 04 'Unia Europejska (27)' (the
   2007-2013 composition) → EUROSTAT:EU27_2007 where Gold has that area. 03 'Unia Europejska (28)' has no Gold area
   and is not built (poc_build_gus_strateg). Level 2: 10 + the TERC voivodeship code + 00000000. Static, so the
   map does not read STRATEG Silver. #}
{% macro poc_geography__gus_strateg() %}
gus_strateg_countries as (
    select * from (
        values
        ('10', 'PL'), ('12', 'AT'), ('14', 'BE'), ('16', 'BG'), ('18', 'HR'), ('20', 'CY'), ('22', 'CZ'),
        ('24', 'DK'), ('26', 'EE'), ('28', 'FI'), ('30', 'FR'), ('32', 'GR'), ('34', 'ES'), ('36', 'NL'),
        ('38', 'IE'), ('40', 'LT'), ('42', 'LU'), ('44', 'LV'), ('46', 'MT'), ('48', 'DE'), ('50', 'PT'),
        ('52', 'RO'), ('54', 'SK'), ('56', 'SI'), ('58', 'SE'), ('60', 'HU'), ('62', 'GB'), ('64', 'IT')
    ) as t (prefix, iso2)
),

gus_strateg as (
    select
        'gus_strateg' as source_system,
        c.prefix || '0000000000' as source_code,
        g.geography_key
    from gus_strateg_countries as c
    inner join geo as g on c.iso2 = g.iso2 and g.level_name = 'country'
    union all
    select
        'gus_strateg' as source_system,
        case g.geography_key when 'EUROSTAT:EU27_2020' then '050000000000' else '040000000000' end as source_code,
        g.geography_key
    from geo as g
    where g.geography_key in ('EUROSTAT:EU27_2020', 'EUROSTAT:EU27_2007')
    union all
    select
        'gus_strateg' as source_system,
        '10' || left(g.terc_code, 2) || '00000000' as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'wojewodztwo'
)
{% endmacro %}
