{# UNECE Statistical Database, economic statistics (gold.build_unece, #2861, #2885): D34 registration
   (docs/source-registration.md). Shapes and codes read from silver.unece_observations by Dev probe 43304159871
   (29 tables, 2,436,893 rows, each table one frequency, no status flags). #}

{# UNECE tables built (block = PXWeb table id as landed; one frequency per table, so one per measure). The other
   20 tables of folder 20-ME (GDP by expenditure and by activity, per capita, deflators, employment by activity,
   the macroeconomic overviews, monthly and quarterly prices and exchange rates, monthly industrial production,
   globalization indicators; about 2.3 million rows) wait for the per-block incremental Gold load (D28: a
   full-rebuild model and the frequency facts stay below 1 M rows). #}
{% macro poc_unece_blocks() %}
    select * from (
        values
        ('40_en_MEUnRateY_r', 'UNECE unemployment rate, annual', 'A'),
        ('55_en_MEUnRateM_r', 'UNECE unemployment rate, monthly', 'M'),
        ('0_en_MECCPriceInterestY_r', 'UNECE consumer and producer price indices, annual', 'A'),
        ('1_en_MECCPriceInterestQ_r', 'UNECE consumer and producer price indices, quarterly', 'Q'),
        ('60_en_MECCWagesY_r', 'UNECE gross average monthly wages, annual', 'A'),
        ('0_en_MECCIndProdY_r', 'UNECE industrial production, annual', 'A'),
        ('10_en_MELFOverviewY_r', 'UNECE population and employment, annual', 'A'),
        ('30_en_MECCExchPPPsNEWY_r', 'UNECE exchange rates and purchasing power parities, annual', 'A'),
        ('30_en_MEEmpTotQ_r', 'UNECE total employment, quarterly', 'Q')
    ) as t (table_id, block_name, frequency)
{% endmacro %}

{# The measures of the built tables: one row per combination of the table's variable value codes (codes = the
   value codes of Indicator, Variable and Measurement joined by '.', empty for a table whose only variable is the
   country). PXWeb has no unit attribute: unit_code is the unit as the value label prints it (per cent for
   rates and growth rates). zero_is_missing: a level (index, persons, wage, exchange rate) cannot be 0, so a
   0 there is a value below the printed precision or a cell without a value and is not loaded (148 cells,
   all 1990-1997, mostly successor states of the USSR in the hyperinflation years; Dev trial 43306220004); a
   rate or growth rate of 0 is an observation.
   tests/unece_measures_match.sql fails when a listed combination has no rows in Silver or a built table
   carries a combination that is not listed. #}
{% macro poc_unece_measures() %}
    select * from (
        values
        ('40_en_MEUnRateY_r', '', 'percent', 'per cent', false, 'Unemployment rate'),
        ('55_en_MEUnRateM_r', 'c0000862', 'percent', 'per cent', false, 'Unemployment rate'),
        ('55_en_MEUnRateM_r', 'c0000863', 'percent', 'per cent', false, 'Unemployment rate, seasonally adjusted'),
        ('0_en_MECCPriceInterestY_r', 'c0000771', 'percent', 'per cent', false, 'Consumer price index, growth rate'),
        ('0_en_MECCPriceInterestY_r', 'c0000772', '2021=100', 'index, 2021 = 100', true, 'Consumer price index, 2021=100'),
        ('0_en_MECCPriceInterestY_r', 'c0000773', 'percent', 'per cent', false, 'Producer price index, growth rate'),
        ('0_en_MECCPriceInterestY_r', 'c0000774', '2021=100', 'index, 2021 = 100', true, 'Producer price index, 2021=100'),
        ('1_en_MECCPriceInterestQ_r', 'c0000767', 'percent', 'per cent', false, 'Consumer price index, growth rate over the same period of the previous year'),
        ('1_en_MECCPriceInterestQ_r', 'c0000768', '2021=100', 'index, 2021 = 100', true, 'Consumer price index, 2021=100'),
        ('1_en_MECCPriceInterestQ_r', 'c0000769', 'percent', 'per cent', false, 'Producer price index, growth rate over the same period of the previous year'),
        ('1_en_MECCPriceInterestQ_r', 'c0000770', '2021=100', 'index, 2021 = 100', true, 'Producer price index, 2021=100'),
        ('60_en_MECCWagesY_r', 'c0000777', 'US$', 'US dollars, at current exchange rates', true, 'Gross average monthly wages, US$ at current exchange rates'),
        ('60_en_MECCWagesY_r', 'c0000778', 'NC', 'national currency, at current prices', true, 'Gross average monthly wages, national currency at current prices'),
        ('0_en_MECCIndProdY_r', 'c0000740', 'percent', 'per cent', false, 'Industrial production index, growth rate'),
        ('0_en_MECCIndProdY_r', 'c0000741', '2021=100', 'index, 2021 = 100', true, 'Industrial production index, 2021=100'),
        ('10_en_MELFOverviewY_r', 'ME.45.c0000802', 'percent', 'per cent', false, 'Total employment, growth rate'),
        ('10_en_MELFOverviewY_r', 'ME.45.c0000803', 'Thousands of persons', 'thousands of persons', true, 'Total employment'),
        ('10_en_MELFOverviewY_r', 'ME.47.c0000802', 'percent', 'per cent', false, 'Total population, growth rate'),
        ('10_en_MELFOverviewY_r', 'ME.47.c0000803', 'Thousands of persons', 'thousands of persons', true, 'Total population'),
        ('30_en_MECCExchPPPsNEWY_r', 'ME.42', 'NCU per US$', 'national currency units per US dollar', true, 'Exchange rate, national currency per US$'),
        ('30_en_MECCExchPPPsNEWY_r', 'ME.43', 'NCU per US$', 'national currency units per US dollar', true, 'Purchasing power parity (PPP), national currency per US$'),
        ('30_en_MECCExchPPPsNEWY_r', 'ME.4869', 'NCU per Ruble', 'national currency units per Russian rouble', true, 'Exchange rate, national currency per rouble'),
        ('30_en_MECCExchPPPsNEWY_r', 'ME.610', 'NCU per Euro', 'national currency units per euro', true, 'Exchange rate, national currency per euro'),
        ('30_en_MEEmpTotQ_r', 'c0000785', 'percent', 'per cent', false, 'Total employment, quarter-on-quarter growth, seasonally adjusted'),
        ('30_en_MEEmpTotQ_r', 'c0000786', 'percent', 'per cent', false, 'Total employment, year-on-year growth'),
        ('30_en_MEEmpTotQ_r', 'c0000787', 'percent', 'per cent', false, 'Total employment, year-on-year growth, seasonally adjusted'),
        ('30_en_MEEmpTotQ_r', 'c0000788', 'Thousands of persons', 'thousands of persons', true, 'Total employment'),
        ('30_en_MEEmpTotQ_r', 'c0000789', 'percent', 'per cent', false, 'Total employment, quarter-on-quarter growth (annualised), seasonally adjusted'),
        ('30_en_MEEmpTotQ_r', 'c0000790', 'Thousands of persons', 'thousands of persons', true, 'Total employment, seasonally adjusted')
    ) as t (table_id, codes, unit_code, unit_name, zero_is_missing, measure_name)
{% endmacro %}

{# The codes of a row of silver.unece_observations (alias o) as poc_unece_measures lists them. One expression
   for the build stage and tests/unece_measures_match.sql. #}
{% macro poc_unece_codes() %}
        concat_ws(
            '.',
            json_extract_string(o.dimensions_json, '$.Indicator'),
            json_extract_string(o.dimensions_json, '$.Variable'),
            json_extract_string(o.dimensions_json, '$.Measurement')
        )
{%- endmacro %}

{# UNECE areas (Country variable of silver.unece_observations, Dev probe 43304159871): the 56 countries by UN M49
   code as published (three digits) → ISO 3166 alpha-2, and the aggregates E27 European Union-27 →
   EUROSTAT:EU27_2020 and EAA euro area → EUROSTAT:EA (as ECB U2 and BIS XM). The UNECE's own groupings have no
   Gold area and are not built (iso2 ''): CIS EECCA, CWG CIS-11, EAEU Eurasian Economic Union, ECE UNECE-52,
   NA North America-2, ST7 Western Balkans-6. Static, so Gold does not read UNECE Silver in poc_map_geography. #}
{% macro poc_unece_areas() %}
    select * from (
        values
        ('008', 'AL'), ('020', 'AD'), ('031', 'AZ'), ('040', 'AT'), ('051', 'AM'), ('056', 'BE'), ('070', 'BA'),
        ('100', 'BG'), ('112', 'BY'), ('124', 'CA'), ('191', 'HR'), ('196', 'CY'), ('203', 'CZ'), ('208', 'DK'),
        ('233', 'EE'), ('246', 'FI'), ('250', 'FR'), ('268', 'GE'), ('276', 'DE'), ('300', 'GR'), ('348', 'HU'),
        ('352', 'IS'), ('372', 'IE'), ('376', 'IL'), ('380', 'IT'), ('398', 'KZ'), ('417', 'KG'), ('428', 'LV'),
        ('438', 'LI'), ('440', 'LT'), ('442', 'LU'), ('470', 'MT'), ('492', 'MC'), ('498', 'MD'), ('499', 'ME'),
        ('528', 'NL'), ('578', 'NO'), ('616', 'PL'), ('620', 'PT'), ('642', 'RO'), ('643', 'RU'), ('674', 'SM'),
        ('688', 'RS'), ('703', 'SK'), ('705', 'SI'), ('724', 'ES'), ('752', 'SE'), ('756', 'CH'), ('762', 'TJ'),
        ('792', 'TR'), ('795', 'TM'), ('804', 'UA'), ('807', 'MK'), ('826', 'GB'), ('840', 'US'), ('860', 'UZ'),
        ('E27', 'EUROSTAT:EU27_2020'), ('EAA', 'EUROSTAT:EA'),
        ('CIS', ''), ('CWG', ''), ('EAEU', ''), ('ECE', ''), ('NA', ''), ('ST7', '')
    ) as t (area_code, iso2)
{% endmacro %}

{% macro poc_source__unece() %}
    {{ return({
        'block_model': 'poc_build_unece',
        'activated': "select 'unece', table_id, block_name from (" ~ poc_unece_blocks() ~ ")",
    }) }}
{% endmacro %}

{# Countries through dim_geography.iso2 (a country Gold does not have stays unmapped and is counted by
   build_check as _X), the two aggregates by their Gold key. #}
{% macro poc_geography__unece() %}
unece_areas as ({{ poc_unece_areas() }}),

unece as (
    select
        'unece' as source_system,
        a.area_code as source_code,
        g.geography_key
    from unece_areas as a
    inner join geo as g on a.iso2 = g.iso2 and g.level_name = 'country'
    where length(a.iso2) = 2
    union all
    select
        'unece' as source_system,
        area_code as source_code,
        iso2 as geography_key
    from unece_areas
    where length(iso2) > 2
)
{% endmacro %}
