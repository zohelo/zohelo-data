{# epo_statistics (register EX-004): Gold registration, decision D34 (docs/source-registration.md). Four blocks
   (models/gold/poc_build_epo_statistics.sql): the country tables, the field tables (all origins), the field x country
   tables and Poland's NUTS 2 regions. #}
{% macro poc_source__epo_statistics() %}
    {{ return({'block_model': 'poc_build_epo_statistics', 'blocks': [
        ['by_country', 'EPO European patent applications and grants by country of residence of the applicant'],
        ['by_field', 'EPO European patent applications and grants by field of technology'],
        ['by_field_country', 'EPO European patent applications and grants by field of technology and country'],
        ['regions', 'EPO European patent applications by NUTS 2 region of Poland'],
    ]}) }}
{% endmacro %}

{# Geography, static (reads only geo and poc_nuts_pl), by code or by the names the workbooks print:
   - the country tables' ISO 3166-1 alpha-2 codes, as Gold's own country list (as wipo_ip_stats); codes without a Gold
     country (historical AN Netherlands Antilles, territories) map to _X with the code kept;
   - 'Total' of the country and field tables (all origins) = the world, WB:WLD;
   - the country names in the header of the field x country tables, each written out below ('Turkey' in the 2019
     grants sheet, 'Türkiye' elsewhere); 'Other countries' and 'Total' there are not built (the world by field comes from
     the field tables);
   - the NUTS 2 regions of the Poland workbook as printed: the 15 that equal a voivodeship by their Polish name (as
     poc_nuts_pl) -> its TERC key; 'Warszawski stoleczny' (printed without the ł) -> NUTS:PL91 and 'Mazowieckie',
     which the sheet prints for the rest of the voivodeship next to Warszawa, -> NUTS:PL92 (Mazowiecki regionalny). #}
{% macro poc_geography__epo_statistics() %}
epo_statistics as (
    select
        'epo_statistics' as source_system,
        g.iso2 as source_code,
        g.geography_key
    from geo as g
    where g.level_name = 'country' and g.iso2 is not null
    union all
    select
        'epo_statistics' as source_system,
        n.source_code,
        n.geography_key
    from (
        values
        ('Total', 'WB:WLD'),
        ('Albania', 'AL'), ('Austria', 'AT'), ('Belgium', 'BE'), ('Bulgaria', 'BG'), ('Switzerland', 'CH'),
        ('Cyprus', 'CY'), ('Czech Republic', 'CZ'), ('Germany', 'DE'), ('Denmark', 'DK'), ('Estonia', 'EE'),
        ('Spain', 'ES'), ('Finland', 'FI'), ('France', 'FR'), ('United Kingdom', 'GB'), ('Greece', 'GR'),
        ('Croatia', 'HR'), ('Hungary', 'HU'), ('Ireland', 'IE'), ('Iceland', 'IS'), ('Italy', 'IT'),
        ('Liechtenstein', 'LI'), ('Lithuania', 'LT'), ('Luxembourg', 'LU'), ('Latvia', 'LV'), ('Monaco', 'MC'),
        ('Montenegro', 'ME'), ('North Macedonia', 'MK'), ('Malta', 'MT'), ('Netherlands', 'NL'), ('Norway', 'NO'),
        ('Poland', 'PL'), ('Portugal', 'PT'), ('Romania', 'RO'), ('Serbia', 'RS'), ('Sweden', 'SE'),
        ('Slovenia', 'SI'), ('Slovakia', 'SK'), ('San Marino', 'SM'), ('Türkiye', 'TR'), ('Turkey', 'TR'),
        ('Australia', 'AU'), ('Canada', 'CA'), ('P.R. China', 'CN'), ('Israel', 'IL'), ('Japan', 'JP'),
        ('R. Korea', 'KR'), ('Taiwan, Province of China', 'TW'), ('United States', 'US'),
        ('Warszawski stoleczny', 'NUTS:PL91'), ('Warszawski stołeczny', 'NUTS:PL91'),
        ('Mazowieckie', 'NUTS:PL92'), ('Mazowiecki regionalny', 'NUTS:PL92')
    ) as n (source_code, geography_key)
    union all
    select
        'epo_statistics' as source_system,
        p.name_pl as source_code,
        g.geography_key
    from ({{ poc_nuts_pl() }}) as p
    inner join geo as g on p.woj_code = g.terc_code and g.level_name = 'wojewodztwo'
    where p.level_name = 'wojewodztwo' and length(p.nuts_code) = 4
)
{% endmacro %}
