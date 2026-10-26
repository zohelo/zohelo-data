{# Gold PoC activation lists (design draft 2, "activation.yaml" in v1): which blocks of each
   source the PoC builds, how a block's codes split into measure and breakdown, and the
   cross-source concepts. Natural string keys everywhere so the model reads without lookups. #}

{% macro poc_eurostat_blocks() %}
    select * from (
        values
        ('DEMO_PJAN', 'Population on 1 January', 'A', 'TOTAL,T'),
        ('DEMO_R_PJANAGGR3', 'Population on 1 January by NUTS 3 region', 'A', 'TOTAL,T'),
        ('DEMO_GIND', 'Population change: demographic balance and crude rates', 'A', ''),
        ('NAMA_10_GDP', 'GDP and main components', 'A', ''),
        ('NAMA_10_PC', 'Main GDP aggregates per capita', 'A', ''),
        ('UNE_RT_A', 'Unemployment by sex and age, annual', 'A', 'Y15-74,T'),
        ('UNE_RT_M', 'Unemployment by sex and age, monthly', 'M', 'Y15-74,SA,T'),
        ('PRC_HICP_MANR', 'HICP, monthly annual rate of change', 'M', 'CP00'),
        ('LFSI_EMP_A', 'Employment and activity by sex and age, annual', 'A', 'Y20-64,T')
    ) as t (dataset_id, block_name, freq_code, total_breakdown)
{% endmacro %}

{# Eurostat dimensions that define the measure (with unit); every other dimension is a breakdown. #}
{% macro poc_eurostat_measure_dims() %}
    select * from (
        values
        ('DEMO_GIND', 'indic_de'),
        ('NAMA_10_GDP', 'na_item'),
        ('NAMA_10_PC', 'na_item'),
        ('LFSI_EMP_A', 'indic_em')
    ) as t (dataset_id, dimension_id)
{% endmacro %}

{% macro poc_bdl_blocks() %}
    select * from (
        values
        ('P2137', 'Ludność wg grup wieku i płci', 'LUDNOŚĆ / STAN LUDNOŚCI'),
        ('P1873', 'Ruch naturalny wg płci', 'LUDNOŚĆ / URODZENIA I ZGONY'),
        ('P2425', 'Gęstość zaludnienia oraz wskaźniki', 'LUDNOŚĆ / STAN LUDNOŚCI'),
        ('P2392', 'Stopa bezrobocia rejestrowanego', 'RYNEK PRACY / BEZROBOCIE REJESTROWANE'),
        ('P2497', 'Przeciętne miesięczne wynagrodzenia brutto', 'WYNAGRODZENIA I ŚWIADCZENIA SPOŁECZNE / WYNAGRODZENIA'),
        ('P3824', 'Mieszkania oddane do użytkowania', 'PRZEMYSŁ I BUDOWNICTWO / BUDOWNICTWO MIESZKANIOWE'),
        ('P1410', 'Powierzchnia', 'PODZIAŁ TERYTORIALNY / POWIERZCHNIA GEODEZYJNA KRAJU (DANE GUGIK)')
    ) as t (subgroup_id, block_name, subject_path)
{% endmacro %}

{# DBW blocks: indicator × section × presentation × periodicity; the files (one per year) share the prefix. #}
{% macro poc_dbw_blocks() %}
    select * from (
        values
        ('581_57_93_4', 'Ludność — makroregiony, regiony, podregiony; charakter miejscowości; płeć; grupy wieku', 'A'),
        ('581_16_2_1', 'Ludność — Polska, dane miesięczne', 'M')
    ) as t (block_id, block_name, freq_code)
{% endmacro %}

{% macro poc_wdi_blocks() %}
    select * from (
        values
        ('SP.POP.TOTL'), ('SP.DYN.LE00.IN'), ('SM.POP.NETM'), ('NY.GDP.MKTP.CD'), ('NY.GDP.PCAP.CD'),
        ('NY.GDP.MKTP.KD.ZG'), ('FP.CPI.TOTL.ZG'), ('SL.UEM.TOTL.ZS'), ('SL.EMP.TOTL.SP.ZS'), ('AG.LND.TOTL.K2')
    ) as t (indicator_code)
{% endmacro %}

{# NUTS 2016/2021 regions of Poland and the TERC voivodeship they equal; PL9 is Mazowieckie
   itself (NUTS 1) and its two NUTS 2 regions are children of the voivodeship. #}
{% macro poc_nuts_pl() %}
    select * from (
        values
        ('PL2', 'Makroregion południowy', null, 'makroregion'),
        ('PL4', 'Makroregion północno-zachodni', null, 'makroregion'),
        ('PL5', 'Makroregion południowo-zachodni', null, 'makroregion'),
        ('PL6', 'Makroregion północny', null, 'makroregion'),
        ('PL7', 'Makroregion centralny', null, 'makroregion'),
        ('PL8', 'Makroregion wschodni', null, 'makroregion'),
        ('PL9', 'Makroregion województwo mazowieckie', '14', 'wojewodztwo'),
        ('PL21', 'Małopolskie', '12', 'wojewodztwo'),
        ('PL22', 'Śląskie', '24', 'wojewodztwo'),
        ('PL41', 'Wielkopolskie', '30', 'wojewodztwo'),
        ('PL42', 'Zachodniopomorskie', '32', 'wojewodztwo'),
        ('PL43', 'Lubuskie', '08', 'wojewodztwo'),
        ('PL51', 'Dolnośląskie', '02', 'wojewodztwo'),
        ('PL52', 'Opolskie', '16', 'wojewodztwo'),
        ('PL61', 'Kujawsko-pomorskie', '04', 'wojewodztwo'),
        ('PL62', 'Warmińsko-mazurskie', '28', 'wojewodztwo'),
        ('PL63', 'Pomorskie', '22', 'wojewodztwo'),
        ('PL71', 'Łódzkie', '10', 'wojewodztwo'),
        ('PL72', 'Świętokrzyskie', '26', 'wojewodztwo'),
        ('PL81', 'Lubelskie', '06', 'wojewodztwo'),
        ('PL82', 'Podkarpackie', '18', 'wojewodztwo'),
        ('PL84', 'Podlaskie', '20', 'wojewodztwo'),
        ('PL91', 'Warszawski stołeczny', '14', 'region'),
        ('PL92', 'Mazowiecki regionalny', '14', 'region')
    ) as t (nuts_code, name_pl, woj_code, level_name)
{% endmacro %}

{# Which makroregion (NUTS 1) a voivodeship belongs to. #}
{% macro poc_woj_makroregion() %}
    select * from (
        values
        ('12', 'PL2'), ('24', 'PL2'), ('30', 'PL4'), ('32', 'PL4'), ('08', 'PL4'), ('02', 'PL5'), ('16', 'PL5'),
        ('04', 'PL6'), ('28', 'PL6'), ('22', 'PL6'), ('10', 'PL7'), ('26', 'PL7'), ('06', 'PL8'), ('18', 'PL8'),
        ('20', 'PL8')
    ) as t (woj_code, nuts1_code)
{% endmacro %}

{# Cross-source concepts: which measures mean the same thing. measure_key patterns are LIKE
   patterns against poc dim_measure.measure_key. #}
{% macro poc_concepts() %}
    select * from (
        values
        ('population', 'Population', 'Ludność', 'persons', 'eurostat', 'eurostat:DEMO_PJAN:NR', 'on 1 January'),
        ('population', 'Population', 'Ludność', 'persons', 'eurostat', 'eurostat:DEMO_R_PJANAGGR3:NR', 'on 1 January, NUTS'),
        ('population', 'Population', 'Ludność', 'persons', 'world_bank_wdi', 'world_bank_wdi:SP.POP.TOTL', 'mid-year'),
        ('population', 'Population', 'Ludność', 'persons', 'gus_bdl', 'gus_bdl:P2137:osoba', 'on 31 December'),
        ('population', 'Population', 'Ludność', 'persons', 'gus_dbw', 'gus_dbw:581_57_93_4:%', 'on 31 December'),
        ('gdp_current_prices', 'GDP, current prices', 'PKB w cenach bieżących', 'million, source currency', 'eurostat', 'eurostat:NAMA_10_GDP:CP_MEUR:B1GQ', 'million EUR'),
        ('gdp_current_prices', 'GDP, current prices', 'PKB w cenach bieżących', 'million, source currency', 'world_bank_wdi', 'world_bank_wdi:NY.GDP.MKTP.CD', 'current US$'),
        ('gdp_per_capita', 'GDP per capita', 'PKB na 1 mieszkańca', 'per person', 'eurostat', 'eurostat:NAMA_10_PC:CP_EUR_HAB:B1GQ', 'EUR per inhabitant'),
        ('gdp_per_capita', 'GDP per capita', 'PKB na 1 mieszkańca', 'per person', 'world_bank_wdi', 'world_bank_wdi:NY.GDP.PCAP.CD', 'current US$'),
        ('gdp_volume_growth', 'GDP volume growth', 'Dynamika realna PKB', '%', 'eurostat', 'eurostat:NAMA_10_GDP:CLV_PCH_PRE:B1GQ', 'chain-linked volumes, % change on previous period'),
        ('gdp_volume_growth', 'GDP volume growth', 'Dynamika realna PKB', '%', 'world_bank_wdi', 'world_bank_wdi:NY.GDP.MKTP.KD.ZG', 'annual %'),
        ('inflation', 'Inflation, consumer prices', 'Inflacja (ceny konsumpcyjne)', '%', 'eurostat', 'eurostat:PRC_HICP_MANR:RCH_A', 'HICP, annual rate of change, monthly'),
        ('inflation', 'Inflation, consumer prices', 'Inflacja (ceny konsumpcyjne)', '%', 'world_bank_wdi', 'world_bank_wdi:FP.CPI.TOTL.ZG', 'CPI, annual %'),
        ('unemployment_rate', 'Unemployment rate (LFS)', 'Stopa bezrobocia (BAEL)', '% of labour force', 'eurostat', 'eurostat:UNE_RT_A:PC_ACT', 'annual, 15-74'),
        ('unemployment_rate', 'Unemployment rate (LFS)', 'Stopa bezrobocia (BAEL)', '% of labour force', 'eurostat', 'eurostat:UNE_RT_M:PC_ACT', 'monthly, seasonally adjusted, 15-74'),
        ('unemployment_rate', 'Unemployment rate (LFS)', 'Stopa bezrobocia (BAEL)', '% of labour force', 'world_bank_wdi', 'world_bank_wdi:SL.UEM.TOTL.ZS', 'ILO modelled estimate'),
        ('registered_unemployment_rate', 'Registered unemployment rate', 'Stopa bezrobocia rejestrowanego', '%', 'gus_bdl', 'gus_bdl:P2392:%', 'registered, end of year'),
        ('employment_rate', 'Employment rate', 'Wskaźnik zatrudnienia', '% of population', 'eurostat', 'eurostat:LFSI_EMP_A:PC_POP:EMP_LFS', '20-64'),
        ('employment_rate', 'Employment rate', 'Wskaźnik zatrudnienia', '% of population', 'world_bank_wdi', 'world_bank_wdi:SL.EMP.TOTL.SP.ZS', '15+, ILO modelled'),
        ('life_expectancy', 'Life expectancy at birth', 'Przeciętne trwanie życia', 'years', 'world_bank_wdi', 'world_bank_wdi:SP.DYN.LE00.IN', 'total'),
        ('net_migration', 'Net migration', 'Saldo migracji', 'persons', 'eurostat', 'eurostat:DEMO_GIND:%:CNMIGRAT', 'incl. statistical adjustment'),
        ('net_migration', 'Net migration', 'Saldo migracji', 'persons', 'world_bank_wdi', 'world_bank_wdi:SM.POP.NETM', ''),
        ('live_births', 'Live births', 'Urodzenia żywe', 'persons', 'eurostat', 'eurostat:DEMO_GIND:%:LBIRTH', ''),
        ('live_births', 'Live births', 'Urodzenia żywe', 'persons', 'gus_bdl', 'gus_bdl:P1873:%', ''),
        ('deaths', 'Deaths', 'Zgony', 'persons', 'eurostat', 'eurostat:DEMO_GIND:%:DEATH', ''),
        ('average_gross_wage', 'Average monthly gross wage', 'Przeciętne miesięczne wynagrodzenie brutto', 'PLN', 'gus_bdl', 'gus_bdl:P2497:%', ''),
        ('dwellings_completed', 'Dwellings completed', 'Mieszkania oddane do użytkowania', 'dwellings', 'gus_bdl', 'gus_bdl:P3824:%', ''),
        ('area', 'Area', 'Powierzchnia', 'km²', 'gus_bdl', 'gus_bdl:P1410:%', 'ha in the source'),
        ('area', 'Area', 'Powierzchnia', 'km²', 'world_bank_wdi', 'world_bank_wdi:AG.LND.TOTL.K2', 'land area'),
        ('population_density', 'Population density', 'Gęstość zaludnienia', 'persons per km²', 'gus_bdl', 'gus_bdl:P2425:%', ''),
        ('fx_rate', 'Exchange rate to PLN', 'Kurs waluty w PLN', 'PLN per unit', 'nbp', 'nbp:fx:A:mid', 'NBP table A, mid'),
        ('gold_price', 'Gold price', 'Cena złota', 'PLN per gram', 'nbp', 'nbp:gold:price', 'NBP fixing, 1000 fineness')
    ) as t (concept_key, name_en, name_pl, unit, source_system, measure_key_pattern, definition)
{% endmacro %}

{# All PoC observations in the common shape (the build stages of the five sources). #}
{% macro poc_observations() %}
    select * from {{ ref('poc_build_eurostat') }}
    union all select * from {{ ref('poc_build_bdl') }}
    union all select * from {{ ref('poc_build_dbw') }}
    union all select * from {{ ref('poc_build_wdi') }}
    union all select * from {{ ref('poc_build_nbp') }}
{% endmacro %}

{# series_key: measure × geography × breakdown codes, readable. #}
{% macro poc_series_key() %}
    measure_key || '|' || geography_key || '|' || breakdown_json
{% endmacro %}
