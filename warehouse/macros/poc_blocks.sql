{# Gold PoC activation lists (design draft 2, "activation.yaml" in v1): which blocks of each
   source the PoC builds, how a block's codes split into measure and breakdown, and the
   cross-source concepts. Natural string keys everywhere so the model reads without lookups. #}

{% macro poc_eurostat_blocks() %}
    select * from (
        values
        ('DEMO_PJAN', 'Population on 1 January', 'A', 'TOTAL,T'),
        ('DEMO_R_PJANAGGR3', 'Population on 1 January by NUTS 3 region', 'A', 'TOTAL,T'),
        ('DEMO_GIND', 'Population change: demographic balance and crude rates', 'A', ''),
        -- zohelo-bi first slice, Polska w skrócie (seeds/kpi_v2_map.csv, #2584)
        ('TRNG_LFSE_01', 'Participation in education and training (last 4 weeks)', 'A', 'Y25-64,T'),
        ('ILC_PEPS01N', 'Persons at risk of poverty or social exclusion by age and sex', 'A', 'T'),
        ('ISOC_SK_DSKL_I21', 'Individuals by level of digital skills (from 2021)', 'A', 'IND_TOTAL'),
        ('IRT_LT_MCBY_M', 'EMU convergence criterion bond yields, monthly', 'M', ''),
        ('NAMA_10R_2GDP', 'GDP at current market prices by NUTS 2 region', 'A', ''),
        ('GOV_10DD_EDPT1', 'Government deficit/surplus, debt and associated data', 'A', 'S13'),
        ('ENV_AIR_GGE', 'Greenhouse gas emissions by source sector', 'A', 'TOTX4_MEMO'),
        ('RD_E_GERDTOT', 'GERD by sector of performance', 'A', 'TOTAL'),
        ('DEMO_MLEXPEC', 'Life expectancy by age and sex', 'A', 'Y_LT1,T'),
        ('DEMO_R_MLIFEXP', 'Life expectancy by age, sex and NUTS 2 region', 'A', 'Y_LT1,T'),
        ('LFSI_NEET_A', 'Young people neither in employment nor in education and training', 'A', 'Y15-29,T'),
        ('TIPSII10', 'Net international investment position, % of GDP', 'A', 'NSA,FA,S1,S1,N_LE,WRL_REST'),
        ('DEMO_PJANIND', 'Population structure indicators at national level', 'A', ''),
        ('DEMO_R_PJANIND2', 'Population structure indicators by NUTS 2 region', 'A', ''),
        ('CEI_WM011', 'Recycling rate of municipal waste', 'A', ''),
        ('ERT_EFF_IC_M', 'Effective exchange rate indices, monthly', 'M', ''),
        ('EDAT_LFSE_03', 'Population by educational attainment level, sex and age (%)', 'A', 'Y25-34,T'),
        ('DEMO_FIND', 'Fertility indicators', 'A', ''),
        ('DEMO_R_FIND2', 'Fertility indicators by NUTS 2 region', 'A', ''),
        ('NAMA_10_GDP', 'GDP and main components', 'A', ''),
        ('NAMA_10_PC', 'Main GDP aggregates per capita', 'A', ''),
        ('UNE_RT_A', 'Unemployment by sex and age, annual', 'A', 'Y15-74,T'),
        ('UNE_RT_M', 'Unemployment by sex and age, monthly', 'M', 'SA,T'),
        ('PRC_HICP_MINR', 'HICP (COICOP 2018), monthly data', 'M', 'TOTAL'),
        ('LFSI_EMP_A', 'Employment and activity by sex and age, annual', 'A', 'Y20-64,T'),
        -- zohelo-bi first slice, every source per KPI (O31, seeds/kpi_v2_map.csv, #2592)
        ('EI_BSIN_M_R2', 'Industry: monthly business survey data', 'M', 'SA'),
        ('EI_BSIN_Q_R2', 'Industry: quarterly business survey data', 'Q', 'SA'),
        ('EI_BSCO_M', 'Consumers: monthly survey data', 'M', 'SA'),
        ('EI_BSSI_M_R2', 'Sentiment indicators, monthly', 'M', 'SA'),
        ('STS_INPR_M', 'Production in industry, monthly', 'M', 'B-D,CA'),
        ('STS_COPR_M', 'Production in construction, monthly', 'M', 'F,CA'),
        ('STS_INPP_M', 'Producer prices in industry, domestic market, monthly', 'M', 'B-E36,NSA'),
        ('STS_TRTU_M', 'Turnover and volume of sales in wholesale and retail trade, monthly', 'M', 'G47,CA'),
        ('BOP_GDP6_Q', 'Main balance of payments and IIP components, % of GDP, quarterly', 'Q', 'NSA,WRL_REST'),
        ('BOP_C6_Q', 'Balance of payments by country, quarterly', 'Q', 'S1,S1,WRL_REST'),
        ('BOP_FDI6_FLOW', 'Direct investment flows by industry, annual', 'A', 'FDI,TOTAL,WRL_REST'),
        ('NAMQ_10_GDP', 'GDP and main components, quarterly', 'Q', 'SCA'),
        ('NAMQ_10_LP_ULC', 'Labour productivity and unit labour costs, quarterly', 'Q', 'SCA'),
        ('JVS_Q_NACE2', 'Job vacancy statistics by NACE Rev. 2, quarterly', 'Q', 'NSA,B-S,TOTAL'),
        ('LFSI_EMP_Q', 'Employment and activity by sex and age, quarterly', 'Q', 'SA,T,Y20-64'),
        ('UNE_RT_Q', 'Unemployment by sex and age, quarterly', 'Q', 'SA,T'),
        ('LFST_R_LFU3RT', 'Unemployment rates by sex, age, education and NUTS 2 region', 'A', 'TOTAL,T,Y15-74'),
        ('UNE_LTU_A', 'Long-term unemployment by sex, annual', 'A', 'Y15-74,T'),
        ('EARN_GR_GPGR2', 'Gender pay gap in unadjusted form', 'A', 'B-S_X_O'),
        ('NAMA_10_AN6', 'Gross fixed capital formation by asset type', 'A', 'N11G'),
        ('NAMA_10_A10', 'Gross value added and income by A*10 industry', 'A', 'TOTAL'),
        ('ILC_DI12', 'Gini coefficient of equivalised disposable income', 'A', 'TOTAL'),
        ('NAMA_10R_2GVAGR', 'Real growth rate of regional gross value added', 'A', ''),
        ('GOV_10A_EXP', 'General government expenditure by function (COFOG)', 'A', 'S13,GF10')
    ) as t (dataset_id, block_name, freq_code, total_breakdown)
{% endmacro %}

{# Eurostat dimensions that define the measure (with unit); every other dimension is a breakdown. #}
{% macro poc_eurostat_measure_dims() %}
    select * from (
        values
        ('DEMO_GIND', 'indic_de'),
        ('NAMA_10_GDP', 'na_item'),
        ('NAMA_10_PC', 'na_item'),
        ('LFSI_EMP_A', 'indic_em'),
        ('ILC_PEPS01N', 'age'),
        ('ISOC_SK_DSKL_I21', 'indic_is'),
        ('IRT_LT_MCBY_M', 'int_rt'),
        ('GOV_10DD_EDPT1', 'na_item'),
        ('ENV_AIR_GGE', 'airpol'),
        ('DEMO_PJANIND', 'indic_de'),
        ('DEMO_R_PJANIND2', 'indic_de'),
        ('CEI_WM011', 'wst_oper'),
        ('ERT_EFF_IC_M', 'exch_rt'),
        ('EDAT_LFSE_03', 'isced11'),
        ('DEMO_FIND', 'indic_de'),
        ('DEMO_R_FIND2', 'indic_de'),
        ('UNE_RT_M', 'age'),
        ('EI_BSIN_M_R2', 'indic'),
        ('EI_BSIN_Q_R2', 'indic'),
        ('EI_BSCO_M', 'indic'),
        ('EI_BSSI_M_R2', 'indic'),
        ('STS_INPR_M', 'indic_bt'),
        ('STS_COPR_M', 'indic_bt'),
        ('STS_INPP_M', 'indic_bt'),
        ('STS_TRTU_M', 'indic_bt'),
        ('BOP_GDP6_Q', 'bop_item'),
        ('BOP_GDP6_Q', 'stk_flow'),
        ('BOP_C6_Q', 'currency'),
        ('BOP_C6_Q', 'bop_item'),
        ('BOP_C6_Q', 'stk_flow'),
        ('BOP_FDI6_FLOW', 'currency'),
        ('BOP_FDI6_FLOW', 'stk_flow'),
        ('BOP_FDI6_FLOW', 'fdi_item'),
        ('NAMQ_10_GDP', 'na_item'),
        ('NAMQ_10_LP_ULC', 'na_item'),
        ('JVS_Q_NACE2', 'indic_em'),
        ('LFSI_EMP_Q', 'indic_em'),
        ('UNE_RT_Q', 'age'),
        ('UNE_LTU_A', 'indic_em'),
        ('NAMA_10_A10', 'na_item'),
        ('ILC_DI12', 'statinfo'),
        ('NAMA_10R_2GVAGR', 'na_item'),
        ('GOV_10A_EXP', 'na_item')
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
        ('P1410', 'Powierzchnia', 'PODZIAŁ TERYTORIALNY / POWIERZCHNIA GEODEZYJNA KRAJU (DANE GUGIK)'),
        -- zohelo-bi first slice, every source per KPI (O31, #2592): Poland and voivodeships only
        ('P2955', 'Wskaźnik cen towarów i usług konsumpcyjnych (COICOP 1999, do 2025)', 'CENY / WSKAŹNIKI CEN'),
        ('P2496', 'Wskaźnik cen towarów i usług konsumpcyjnych (COICOP 1999, kwartalne, do 2025)', 'CENY / WSKAŹNIKI CEN'),
        ('P4635', 'Wskaźniki cen towarów i usług konsumpcyjnych (COICOP 2018, kwartalne, od 2026)', 'CENY / WSKAŹNIKI CEN'),
        ('P2359', 'Współczynnik aktywności zawodowej (BAEL, średnioroczne)', 'RYNEK PRACY / AKTYWNOŚĆ EKONOMICZNA (BAEL)'),
        ('P2360', 'Wskaźnik zatrudnienia (BAEL, średnioroczne)', 'RYNEK PRACY / AKTYWNOŚĆ EKONOMICZNA (BAEL)'),
        ('P2361', 'Stopa bezrobocia (BAEL, średnioroczne)', 'RYNEK PRACY / AKTYWNOŚĆ EKONOMICZNA (BAEL)'),
        ('P2961', 'Bezrobotni zarejestrowani wg płci (dane miesięczne)', 'RYNEK PRACY / BEZROBOCIE REJESTROWANE'),
        ('P4334', 'Zarejestrowani bezrobotni na 1 ofertę pracy', 'RYNEK PRACY / BEZROBOCIE REJESTROWANE'),
        ('P2730', 'Przeciętne dalsze trwanie życia', 'LUDNOŚĆ / TRWANIE ŻYCIA'),
        ('P2346', 'Współczynnik dzietności i reprodukcji brutto', 'LUDNOŚĆ / URODZENIA I ZGONY'),
        ('P2426', 'Wskaźniki obciążenia demograficznego', 'LUDNOŚĆ / STAN LUDNOŚCI'),
        ('P1869', 'Przeciętny miesięczny dochód rozporządzalny na 1 osobę', 'BUDŻETY GOSPODARSTW DOMOWYCH'),
        ('P2861', 'Emeryci i renciści', 'WYNAGRODZENIA I ŚWIADCZENIA SPOŁECZNE / EMERYTURY I RENTY'),
        ('P2860', 'Przeciętna miesięczna emerytura i renta brutto', 'WYNAGRODZENIA I ŚWIADCZENIA SPOŁECZNE / EMERYTURY I RENTY'),
        ('P2862', 'Nakłady inwestycyjne i wartość brutto środków trwałych', 'INWESTYCJE I ŚRODKI TRWAŁE'),
        ('P3502', 'Produkt krajowy brutto ogółem, dynamika', 'RACHUNKI REGIONALNE / PRODUKT KRAJOWY BRUTTO')
    ) as t (subgroup_id, block_name, subject_path)
{% endmacro %}

{# DBW blocks: indicator × section × presentation × periodicity; the files (one per year) share the prefix. #}
{% macro poc_dbw_blocks() %}
    select * from (
        values
        ('581_57_93_4', 'Ludność — makroregiony, regiony, podregiony; charakter miejscowości; płeć; grupy wieku', 'A'),
        ('581_16_2_1', 'Ludność — Polska, dane miesięczne', 'M'),
        -- zohelo-bi first slice, every source per KPI (O31, #2592); file stem without year
        ('305_739_5_1', 'Wskaźniki cen towarów i usług konsumpcyjnych — analogiczny okres = 100, do 2025', 'M'),
        ('305_1722_5_1', 'Wskaźniki cen towarów i usług konsumpcyjnych — analogiczny okres = 100, od 2026', 'M'),
        ('305_1721_5_3', 'Wskaźniki cen towarów i usług konsumpcyjnych — województwa, kwartalne, od 2026', 'Q'),
        ('376_2_243_1', 'Przeciętne miesięczne wynagrodzenie brutto w sektorze przedsiębiorstw [zł]', 'M'),
        ('376_2_184_1', 'Wynagrodzenie w sektorze przedsiębiorstw — analogiczny okres = 100, nominalnie', 'M'),
        ('376_16_182_1', 'Wynagrodzenie w sektorze przedsiębiorstw — analogiczny okres = 100, realnie', 'M'),
        ('398_16_243_3', 'Przeciętne miesięczne wynagrodzenie brutto w gospodarce narodowej [zł]', 'Q'),
        ('398_16_184_3', 'Wynagrodzenie w gospodarce narodowej — analogiczny okres = 100, nominalnie', 'Q'),
        ('398_16_182_3', 'Wynagrodzenie w gospodarce narodowej — analogiczny okres = 100, realnie', 'Q'),
        ('875_143_95_1', 'Stopa bezrobocia rejestrowanego — Polska, województwa, powiaty', 'M'),
        ('1514_1360_95_3', 'Stopa bezrobocia - BAEL — Polska, makroregiony, województwa, regiony', 'Q'),
        ('1513_1360_95_3', 'Wskaźnik zatrudnienia - BAEL — Polska, makroregiony, województwa, regiony', 'Q'),
        ('1512_1360_95_3', 'Współczynnik aktywności zawodowej - BAEL — Polska, makroregiony, województwa, regiony', 'Q'),
        ('1038_16_389_3', 'PKB — analogiczny okres = 100, ceny stałe 2020', 'Q'),
        ('1038_16_390_3', 'PKB — okres poprzedni = 100, wyrównany sezonowo, ceny stałe 2020', 'Q'),
        ('1038_16_92_4', 'PKB — rok poprzedni = 100, ceny stałe', 'A'),
        ('1038_16_105_4', 'PKB — ceny bieżące [mln zł]', 'A'),
        ('1173_793_92_4', 'PKB w ujęciu regionalnym — rok poprzedni = 100, ceny stałe', 'A'),
        ('814_2_7_1', 'Produkcja sprzedana przemysłu — analogiczny okres = 100, ceny stałe', 'M'),
        ('392_16_223_1', 'Produkcja budowlano-montażowa — analogiczny okres = 100, wyrównana sezonowo', 'M'),
        ('109_849_7_1', 'Sprzedaż detaliczna towarów — analogiczny okres = 100, ceny stałe', 'M'),
        ('469_16_117_1', 'Bieżący wskaźnik ufności konsumenckiej (BWUK)', 'M')
    ) as t (block_id, block_name, freq_code)
{% endmacro %}

{% macro poc_wdi_blocks() %}
    select * from (
        values
        ('SP.POP.TOTL'), ('SP.DYN.LE00.IN'), ('SM.POP.NETM'), ('NY.GDP.MKTP.CD'), ('NY.GDP.PCAP.CD'),
        ('NY.GDP.MKTP.KD.ZG'), ('FP.CPI.TOTL.ZG'), ('SL.UEM.TOTL.ZS'), ('SL.EMP.TOTL.SP.ZS'), ('AG.LND.TOTL.K2'),
        -- zohelo-bi first slice, every source per KPI (O31, #2592)
        ('SL.TLF.ACTI.ZS'), ('SL.UEM.1524.ZS'), ('SL.UEM.NEET.ZS'), ('GB.XPD.RSDV.GD.ZS'), ('SP.DYN.TFRT.IN'),
        ('SP.POP.DPND.OL'), ('SE.TER.CUAT.BA.ZS'), ('GC.NLD.TOTL.GD.ZS'), ('EN.GHG.ALL.MT.CE.AR5'),
        ('BN.CAB.XOKA.GD.ZS'), ('BX.KLT.DINV.CD.WD'), ('SI.POV.GINI'), ('NE.GDI.FTOT.ZS'), ('PX.REX.REER'),
        ('NY.GDP.PCAP.PP.CD'), ('NY.GDP.MKTP.CN')
    ) as t (indicator_code)
{% endmacro %}

{# OECD composite leading and confidence indicators (silver.oecd_cli, #2738): the measures Gold builds.
   Amplitude adjusted (AA), index points, long-term average = 100. The CLI (LI) covers 22 areas, not
   Poland or the EU; business (BCICP) and consumer (CCICP) confidence cover Poland. #}
{% macro poc_oecd_measures() %}
    select * from (
        values
        ('BCICP', 'AA', 'IX', 'Business confidence indicator (BCI), amplitude adjusted, long-term average = 100'),
        ('CCICP', 'AA', 'IX', 'Consumer confidence indicator (CCI), amplitude adjusted, long-term average = 100'),
        ('LI', 'AA', 'IX', 'Composite leading indicator (CLI), amplitude adjusted, long-term average = 100')
    ) as t (measure, adjustment, transformation, measure_name)
{% endmacro %}

{# OECD area codes that are not ISO 3166 alpha-3 countries: their names, and the Eurostat aggregate they equal
   (EU27_2020 only: Gold has no Eurostat EA20 member, so the OECD euro areas are OECD:<code> members). #}
{% macro poc_oecd_areas() %}
    select * from (
        values
        ('EU27_2020', 'European Union (27 countries from 2020)', 'EU27_2020'),
        ('EA20', 'Euro area (20 countries)', ''),
        ('EA19', 'Euro area (19 countries)', ''),
        ('EU', 'European Union (OECD composite)', ''),
        ('OECDXEA', 'OECD excluding the euro area', ''),
        ('OECD', 'OECD total', ''),
        ('OECDE', 'OECD Europe', ''),
        ('G7', 'G7', ''),
        ('G20', 'G20', ''),
        ('A5M', 'Major five Asia economies', ''),
        ('G4E', 'Major four European economies', ''),
        ('NAFTA', 'NAFTA', '')
    ) as t (ref_area, name_en, eurostat_code)
{% endmacro %}

{# World Bank WGI: the six governance estimates (#2736). #}
{% macro poc_wgi_blocks() %}
    select * from (
        values
        ('GOV_WGI_VA.EST'), ('GOV_WGI_PV.EST'), ('GOV_WGI_GE.EST'), ('GOV_WGI_RQ.EST'), ('GOV_WGI_RL.EST'),
        ('GOV_WGI_CC.EST')
    ) as t (indicator_code)
{% endmacro %}

{# World Bank Global Findex: account ownership, age 15+ (#2736). #}
{% macro poc_findex_blocks() %}
    select * from (
        values
        ('account_t_d', 'Account ownership (% age 15+)')
    ) as t (indicator, block_name)
{% endmacro %}

{# World Bank ICP 2021: GDP per capita in PPP and the GDP price level index (#2736). #}
{% macro poc_icp_blocks() %}
    select * from (
        values
        ('1000000', 'PCAP.PP', 'ICP GDP per capita, PPP-based (international $)'),
        ('1000000', 'PX.WL', 'ICP GDP price level index (world = 100)')
    ) as t (series_code, classification_code, block_name)
{% endmacro %}

{# UNCTAD Data Hub blocks (gold.build_unctad): table × dimension codes (dimension_names, in that order) × measure.
   measure_field is the table's measure column; measure_suffix its short code in the measure key. #}
{% macro poc_unctad_blocks() %}
    select * from (
        values
        ('US_FdiFlowsStock', 'FDI inward flows (UNCTAD), US$ million', 'Flow,Direction', '08:1', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_FdiFlowsStock', 'FDI outward flows (UNCTAD), US$ million', 'Flow,Direction', '08:2', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_FdiFlowsStock', 'FDI inward stock (UNCTAD), US$ million', 'Flow,Direction', '09:1', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_FdiFlowsStock', 'FDI outward stock (UNCTAD), US$ million', 'Flow,Direction', '09:2', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_FdiFlowsStock', 'FDI inward stock (UNCTAD), % of GDP', 'Flow,Direction', '09:1', 'Percentage of Gross Domestic Product', 'PCT_GDP', 'PC_GDP', 'percent of GDP'),
        ('US_TradeMerchTotal', 'Merchandise exports (UNCTAD), US$ million', 'Flow', '02', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_TradeMerchTotal', 'Merchandise imports (UNCTAD), US$ million', 'Flow', '01', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_TradeMerchTotal', 'Merchandise exports (UNCTAD), growth year on year', 'Flow', '02', 'Growth rate, year-on-year', 'GROWTH', 'PCH', 'percent change on previous year'),
        ('US_TradeServCatTotal', 'Services exports (UNCTAD), US$ million', 'Flow,Category', '02:S', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices'),
        ('US_TradeServCatTotal', 'Services imports (UNCTAD), US$ million', 'Flow,Category', '01:S', 'Millions of US$ at current prices', 'USD_MN', 'MIO_USD', 'US$ million at current prices')
    ) as t (entity, block_name, dimension_names, dimension_codes, measure_field, measure_suffix, unit_code, unit_name)
{% endmacro %}

{# World Bank IDS series built (gold.build_ids, #2819): external debt stocks and debt service, in US$ and as
   ratios. Debtor economy × counterpart WLD (the only counterpart in the archive); annual. #}
{% macro poc_ids_series() %}
    select * from (
        values
        ('DT.DOD.DECT.CD', 'usd'),
        ('DT.DOD.DECT.GN.ZS', 'pct_gni'),
        ('DT.TDS.DECT.CD', 'usd'),
        ('DT.TDS.DECT.GN.ZS', 'pct_gni'),
        ('DT.TDS.DECT.EX.ZS', 'pct_exports')
    ) as t (series_code, unit_code)
{% endmacro %}

{# UNCTAD economies built (gold.build_unctad): EU27 members by UNCTAD code, the European Union (2020 …) group and World. #}
{% macro poc_unctad_areas() %}
    select * from (
        values
        ('040', 'AT'), ('056', 'BE'), ('100', 'BG'), ('191', 'HR'), ('196', 'CY'), ('203', 'CZ'), ('208', 'DK'),
        ('233', 'EE'), ('246', 'FI'), ('250', 'FR'), ('276', 'DE'), ('300', 'GR'), ('348', 'HU'), ('372', 'IE'),
        ('380', 'IT'), ('428', 'LV'), ('440', 'LT'), ('442', 'LU'), ('470', 'MT'), ('528', 'NL'), ('616', 'PL'),
        ('620', 'PT'), ('642', 'RO'), ('703', 'SK'), ('705', 'SI'), ('724', 'ES'), ('752', 'SE'),
        ('3412', 'EUROSTAT:EU27_2020'), ('0000', 'WB:WLD')
    ) as t (economy, candidate_key)
{% endmacro %}

{# WHO data.who.int blocks (gold.build_who): one per indicator file; areas: EU27 members (M49, macro
   poc_m49_eu27) and World (M49 1, GLOBAL). #}
{% macro poc_who_blocks() %}
    select * from (
        values
        ('life_expectancy', 'Life expectancy at birth (WHO)'),
        ('hale', 'Healthy life expectancy at birth (WHO)')
    ) as t (entity, block_name)
{% endmacro %}

{# UNESCO UIS blocks (gold.build_unesco_uis): one per Data API selection; areas: EU27 members (ISO alpha-3 through
   dim_geography.iso3). #}
{% macro poc_unesco_uis_blocks() %}
    select * from (
        values
        ('gerd_gdp', 'EXPGDP.TOT', 'GERD as a percentage of GDP (UIS)', 'pct_gdp'),
        ('tertiary_attainment', 'EA.5T8.AG25T99', 'Completed short-cycle tertiary or higher, population 25+ (UIS)', 'pct')
    ) as t (entity, indicator_id, block_name, unit_code)
{% endmacro %}

{# FAOSTAT blocks (gold.build_faostat): one per domain item × element; areas: EU27 members (M49, macro
   poc_m49_eu27), FAO aggregates 5707 European Union (27) and 5000 World. #}
{% macro poc_faostat_blocks() %}
    select * from (
        values
        ('emissions_totals', '6829', '723113', 'GHG emissions, all sectors without LULUCF, CO2eq (AR5) (FAOSTAT)'),
        ('land_use', '6601', '5110', 'Land area (FAOSTAT)')
    ) as t (entity, item_code, element_code, block_name)
{% endmacro %}

{# ECB Data Portal blocks: one per landed selection (ingestion/sources/ecb.yaml entity). currencies: the EXR
   currencies built (EU members outside the euro area and the main world currencies); empty for other flows. #}
{% macro poc_ecb_blocks() %}
    select * from (
        values
        ('exr_daily', 'ECB euro reference exchange rates, daily', 'PLN,CZK,HUF,RON,SEK,DKK,BGN,USD,GBP,CHF,JPY,CNY,NOK'),
        ('exr_monthly', 'ECB euro reference exchange rates, monthly average', 'PLN,CZK,HUF,RON,SEK,DKK,BGN,USD,GBP,CHF,JPY,CNY,NOK'),
        ('fm_key_rates', 'Key ECB interest rates (date of changes)', ''),
        ('est_estr', 'Euro short-term rate (EUR STR)', ''),
        ('icp_annual_rate', 'HICP annual rate of change (ICP, discontinued after 2025-12)', ''),
        ('hicp_annual_rate', 'HICP annual rate of change (HICP, COICOP 2018)', ''),
        ('irs_long_term', 'Long-term interest rates for convergence purposes (10-year)', ''),
        ('bsi_monetary_aggregates', 'Euro area monetary aggregates M1, M2, M3', '')
    ) as t (entity, block_name, currencies)
{% endmacro %}

{# ECB area codes → Gold area (poc_map_geography section ecb; static, so Gold does not depend on ECB Silver
   existing). Countries are ISO two-letter codes and map by themselves; these are the aggregates (TITLE_COMPL in
   silver.ecb_observations, Dev trial 43088143150). I8/I9 use the keys Eurostat's EA19/EA20 have today
   (NUTS:EA19, NUTS:EA20). I10 (euro area 21) and V5 have no Gold area yet. #}
{% macro poc_ecb_areas() %}
    select * from (
        values
        ('U2', 'EUROSTAT:EA'), ('I8', 'NUTS:EA19'), ('I9', 'NUTS:EA20'), ('B6', 'EUROSTAT:EU27_2020'),
        ('B0', 'EUROSTAT:EU'), ('D0', 'EUROSTAT:EU')
    ) as t (area_code, candidate_key)
{% endmacro %}

{# UN WPP blocks: one per indicator (gold.build_un_wpp); countries: the EU27 members built (ISO two-letter). #}
{% macro poc_un_wpp_blocks() %}
    select
        *,
        'AT,BE,BG,CY,CZ,DE,DK,EE,ES,FI,FR,GR,HR,HU,IE,IT,LT,LU,LV,MT,NL,PL,PT,RO,SE,SI,SK' as countries
    from (
        values
        ('TPopulation1July', 'Total population on 1 July (UN WPP)', 'thousand_persons', 'thousand persons'),
        ('TFR', 'Total fertility rate (UN WPP)', 'children_per_woman', 'live births per woman'),
        ('LEx', 'Life expectancy at birth, both sexes (UN WPP)', 'years', 'years'),
        ('MedianAgePop', 'Median age of the population (UN WPP)', 'years', 'years'),
        ('OldAgeDependency', 'Old-age dependency ratio, 65+ per 100 aged 15-64, computed from UN WPP 2024 five-year age groups', 'per_100', 'per 100 persons aged 15-64')
    ) as t (indicator, block_name, unit_code, unit_name)
{% endmacro %}

{# UN WPP groups → Gold area by LocID (poc_map_geography section un_wpp; static). Countries map by ISO code. #}
{% macro poc_un_wpp_areas() %}
    select * from (
        values
        ('1551', 'EUROSTAT:EU27_2020'), ('900', 'WB:WLD')
    ) as t (loc_id, candidate_key)
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
    {# KPI definitions are seeds (warehouse/seeds/kpi.csv, kpi_measure.csv, unit.csv). #}
    select
        k.kpi_key as concept_key,
        k.name_en,
        k.name_pl,
        u.name_en as unit,
        m.source_system,
        m.measure_key_pattern,
        m.definition
    from {{ ref('kpi_measure') }} as m
    inner join {{ ref('kpi') }} as k on m.kpi_key = k.kpi_key
    inner join {{ ref('unit') }} as u on k.unit_key = u.unit_key
    order by k.kpi_key, m.priority
{% endmacro %}

{# The Gold blocks (build stage models) as `model:source_system`, one per line. A block model carries the tag
   `gold_block` (ops/repo_check.py checks both lists agree): its children do not wait for it in the
   changed-only build (warehouse/changed.py), they read it only once its table exists. #}
{% macro poc_block_models() %}
    poc_build_eurostat:eurostat
    poc_build_bdl:gus_bdl
    poc_build_dbw:gus_dbw
    poc_build_wdi:world_bank_wdi
    poc_build_nbp:nbp
    poc_build_unctad:unctad
    poc_build_wgi:world_bank_wgi
    poc_build_findex:world_bank_findex
    poc_build_icp:world_bank_icp
    poc_build_public_finance:mf_public_debt
    poc_build_oecd:oecd
    poc_build_gleif:gleif
    poc_build_mf_biala_lista:mf_biala_lista
    poc_build_prg:gugik_prg
    poc_build_mf_jst_budgets:mf_jst_budgets
    poc_build_mrpips_unemployment:mrpips_unemployment
    poc_build_who:who
    poc_build_unesco_uis:unesco_uis
    poc_build_faostat:faostat
    poc_build_ecb:ecb
    poc_build_ids:world_bank_ids
    poc_build_un_wpp:un_wpp
    poc_build_pig_midas:pig_midas
{% endmacro %}

{# All Gold observations in the common shape: the union of the Gold blocks. A block without a table is
   left out (zero rows) only when the changed-only plan declared it waiting: warehouse/changed.py lists
   it under `empty_blocks` (its source tables do not exist yet) and warehouse/build.py passes that list
   as the dbt var `waiting_blocks`. Any other missing block (a partial catalog listing, a run killed
   between renames, an explicit-filter build) is a compiler error, so a source never drops out of Gold
   silently; a block built in this run that fails makes dbt skip every model reading it. At parse time
   every block is ref'd, so dbt records each dependency. #}
{% macro poc_observations() %}
    {% set ns = namespace(first=true) %}
    {% for entry in poc_block_models().split() %}
        {% set relation = ref(entry.split(':')[0]) %}
        {% if not execute or not (poc_block_waiting(entry, relation) | trim) %}
            {% if not ns.first %}union all {% endif %}select * from {{ relation }}
            {% set ns.first = false %}
        {% endif %}
    {% endfor %}
    {% if ns.first %}
        {% do exceptions.raise_compiler_error('poc_observations: no Gold block table exists') %}
    {% endif %}
{% endmacro %}

{# 'waiting' (rendered) when the block of `entry` (model:source_system) has no table and the plan declared
   it waiting; '' when its table exists; a compiler error when it has no table and was not declared. #}
{% macro poc_block_waiting(entry, relation) %}
    {%- set model = entry.split(':')[0] -%}
    {%- if load_relation(relation) is none -%}
        {%- if model in var('waiting_blocks', []) -%}
            {%- do log('Gold block ' ~ relation ~ ' is waiting (source tables do not exist yet): zero rows', info=true) -%}
            waiting
        {%- else -%}
            {%- do exceptions.raise_compiler_error(
                'Gold block ' ~ model ~ ' (' ~ relation ~ ') does not exist and changed.py did not declare it '
                ~ 'waiting (var waiting_blocks): build it first, e.g. select ' ~ model ~ '+'
            ) -%}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}

{# The source_system of each waiting block (poc_block_waiting), quoted and comma-separated ('' at parse
   time). #}
{% macro poc_absent_blocks() %}
    {%- set absent = [] -%}
    {%- for entry in poc_block_models().split() -%}
        {%- if execute and poc_block_waiting(entry, ref(entry.split(':')[0])) | trim -%}
            {%- do absent.append("'" ~ entry.split(':')[1] ~ "'") -%}
        {%- endif -%}
    {%- endfor -%}
    {{- absent | join(', ') -}}
{% endmacro %}

{# series_key: measure × geography × breakdown codes, readable; an unmatched area keeps its source code so
   two unmatched units never share a series. #}
{% macro poc_series_key() %}
    measure_key || '|' || geography_key
    || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end
    || '|' || breakdown_json
{% endmacro %}

{# Every activated block of every source: the list build_check reports against, so a block that
   yields no rows (not in Silver yet, or filtered out) is visible. #}
{% macro poc_activated_blocks() %}
    select 'eurostat' as source_system, dataset_id as block_id, block_name from ({{ poc_eurostat_blocks() }})
    union all
    select 'gus_bdl', subgroup_id, block_name from ({{ poc_bdl_blocks() }})
    union all
    select 'gus_dbw', block_id, block_name from ({{ poc_dbw_blocks() }})
    union all
    select 'world_bank_wdi', indicator_code, 'WDI ' || indicator_code from ({{ poc_wdi_blocks() }})
    union all
    select 'world_bank_wgi', indicator_code, 'WGI ' || indicator_code from ({{ poc_wgi_blocks() }})
    union all
    select 'world_bank_findex', indicator, block_name from ({{ poc_findex_blocks() }})
    union all
    select 'world_bank_icp', series_code || ':' || classification_code, block_name from ({{ poc_icp_blocks() }})
    union all
    select distinct 'unctad', entity, entity from ({{ poc_unctad_blocks() }})
    union all
    select 'who', entity, block_name from ({{ poc_who_blocks() }})
    union all
    select 'unesco_uis', entity, block_name from ({{ poc_unesco_uis_blocks() }})
    union all
    select 'faostat', entity || ':' || item_code || '.' || element_code, block_name from ({{ poc_faostat_blocks() }})
    union all
    select 'ecb', entity, block_name from ({{ poc_ecb_blocks() }})
    union all
    select 'world_bank_ids', 'IDS_CSV', 'World Bank International Debt Statistics'
    union all
    select 'un_wpp', indicator, block_name from ({{ poc_un_wpp_blocks() }})
    union all
    select * from (
        values
        ('nbp', 'fx_A', 'NBP exchange rates, table A'),
        ('nbp', 'fx_C', 'NBP exchange rates, table C'),
        ('nbp', 'gold', 'NBP gold fixings'),
        ('mf_public_debt', 'debt_gdp', 'Public debt in relation to GDP (MF)'),
        ('mf_public_debt', 'debt', 'Public debt (MF)'),
        ('mf_public_debt', 'edp_debt', 'General government (EDP) debt by instrument (MF)'),
        ('mf_treasury_debt', 'debt_instrument', 'State Treasury debt by instrument (MF)'),
        ('mf_state_budget', 'execution_ytd', 'State budget execution, cumulative (MF)'),
        ('mf_state_budget', 'plan', 'State budget act (MF)'),
        ('krus', 'benefits', 'KRUS pensions and benefits'),
        ('oecd', 'cli', 'OECD composite leading indicators and confidence indicators'),
        ('gleif', 'lei2', 'GLEIF LEI records (golden copy)'),
        ('mf_biala_lista', 'flat_file', 'MF VAT white list, flat file'),
        ('gugik_prg', 'jednostki_administracyjne', 'PRG administrative units'),
        ('mf_jst_budgets', 'jst_budget', 'MF local-government budget summaries (year to date)'),
        ('mrpips_unemployment', 'registered_unemployment', 'MRPiPS registered unemployment (end of month)'),
        ('pig_midas', 'documented_deposits', 'PIG-PIB MIDAS documented mineral deposits'),
        ('pig_midas', 'mining_areas', 'PIG-PIB MIDAS mining areas'),
        ('pig_midas', 'mining_terrains', 'PIG-PIB MIDAS mining terrains')
    ) as t (source_system, block_id, block_name)
{% endmacro %}
