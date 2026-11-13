-- Gold metrics: every value lies within a plausible range for its KPI, source and measure. The bounds
-- catch unit and scale errors (a value ×1000, a percentage stored as a share), not small mistakes.
-- Change a bound only with the publisher's own value as evidence, cited in a comment next to it.
-- A bound covers the measures of the KPI × source whose key matches its pattern ('%' = all of them);
-- measures in different units (million EUR and million national currency) get a row each, and the
-- patterns of one KPI × source never overlap. A measure without a bound fails, so a new source or
-- measure gets one here.
with bounds (concept_key, source_system, measure_key_pattern, value_min, value_max) as (
    values
    ('population', 'gus_bdl', '%', 0, 5e7),
    ('population', 'gus_dbw', '%', 0, 5e7),
    -- persons; largest area EEA31 2020 = 519,826,027 (Eurostat demo_pjan, #2788)
    ('population', 'eurostat', '%', 0, 1e9),
    -- persons; largest area WLD 2025 = 8,215,424,893 (World Bank API SP.POP.TOTL, #2788)
    ('population', 'world_bank_wdi', '%', 0, 1e10),
    -- thousand persons as published (D33); largest area World, WPP 2024 medium-variant peak about 10.3 million
    -- thousand in the 2080s (10,180,160.751 in 2100; Dev trial 43098260601)
    ('population_projection', 'un_wpp', '%', 0, 2e7),
    ('gdp_current_prices', 'eurostat', 'eurostat:NAMA_10_GDP:CP_MEUR:B1GQ', 0, 3e7),
    -- million national currency; largest HU 2025 = 87,307,647 million HUF (Eurostat nama_10_gdp, #2788)
    ('gdp_current_prices', 'eurostat', 'eurostat:NAMA_10_GDP:CP_MNAC:B1GQ', 0, 1e9),
    -- million PLN, Poland only; 2025 = 3.915e6 (GUS DBW 1038_16_105_4; Eurostat CP_MNAC PL is the same order)
    ('gdp_current_prices', 'gus_dbw', '%', 0, 3e7),
    -- million US$ since D33 (metrics convert the published US$ by scale): the former bound 3e14 US$ / 1e6
    ('gdp_current_prices', 'world_bank_wdi', 'world_bank_wdi:NY.GDP.MKTP.CD', 0, 3e8),
    -- current local currency units; largest IR 2025 = 2.693e17 IRR as published (WDI NY.GDP.MKTP.CN, #2788)
    ('gdp_current_prices', 'world_bank_wdi', 'world_bank_wdi:NY.GDP.MKTP.CN', 0, 3e18),
    -- vehicles registered in a month (#2966); Poland is the largest area: 206,017 in March 2026 (CEPiK workbook
    -- of 22 Oct 2026, Dev probe 43340837086); a monthly count above 1 million is a scale error
    ('vehicle_registrations', 'cepik', '%', 0, 1e6),
    -- GUS publication tables (#2893), counts per year; Poland is the largest area: museum visits 46,242,767 (2025),
    -- cinema admissions 61,949,490 (2019), library users 5,983,725 (2019), tourists 42,738,329, overnight stays
    -- 104,692,301, bed places 887,158 (2025) (Dev trial 43298219762); survey shares are % of persons 15+
    ('museum_visitors', 'gus_culture', '%', 0, 2e8),
    ('cinema_admissions', 'gus_culture', '%', 0, 2e8),
    ('public_library_users', 'gus_culture', '%', 0, 3e7),
    ('tourists_accommodated', 'gus_tourism', '%', 0, 2e8),
    ('tourist_overnight_stays', 'gus_tourism', '%', 0, 5e8),
    ('tourist_bed_places', 'gus_tourism', '%', 0, 5e6),
    ('museum_visiting_share', 'gus_culture_participation', '%', 0, 100),
    ('theatre_attendance_share', 'gus_culture_participation', '%', 0, 100),
    -- GUS census tables (#2897): urban share in %; holdings PL 2010 = 1,509,148; ha per holding, highest 22.22
    -- (lubuskie 2020) (Dev trial 43288120956)
    ('urban_population_share', 'gus_nsp2021', '%', 0, 100),
    ('agricultural_holdings', 'gus_psr2020', '%', 0, 3e6),
    ('average_farm_area', 'gus_psr2020', '%', 0, 100),
    -- MRPiPS-03 (#2910): persons granted social assistance, PL 2019 = 1,205,506 (Dev trial 43293740538)
    ('social_assistance_recipients', 'mrpips_social_assistance', '%', 0, 1e7),
    ('inflation', 'eurostat', '%', -100, 3000),
    ('inflation', 'world_bank_wdi', '%', -100, 100000),
    ('unemployment_rate', 'eurostat', '%', 0, 60),
    ('unemployment_rate', 'world_bank_wdi', '%', 0, 60),
    -- percent of the labour force (LFS/BAEL); DBW 1514_1360_95_3 1.3-6.5, BDL P2361 up to 14.3 (#2788)
    ('unemployment_rate', 'gus_dbw', '%', 0, 60),
    ('unemployment_rate', 'gus_bdl', '%', 0, 60),
    ('fx_rate', 'nbp', '%', 0, 100000),
    -- ECB (#2758): the ECB's HICP is Eurostat's HICP republished (TITLE_COMPL "Statistical Office of the European
    -- Commission (Eurostat)"), so it takes Eurostat's bound; EUR/PLN monthly average 4.3813 in 2026-10 and daily
    -- 4.0712-4.4038 since 1999 (EXR, Dev trial 43099368908), NBP's bound
    ('inflation', 'ecb', '%', -100, 3000),
    ('fx_rate', 'ecb', '%', 0, 100000),
    ('gdp_per_capita', 'eurostat', '%', 0, 2e5),
    ('gdp_per_capita', 'world_bank_wdi', '%', 0, 5e5),
    ('gdp_volume_growth', 'eurostat', '%', -50, 100),
    ('gdp_volume_growth', 'world_bank_wdi', '%', -100, 200),
    -- an index, previous period or same quarter of the previous year = 100 (rate = value - 100);
    -- Poland published 92.6-111.2 (GUS DBW 1038_16_389_3) and 98.0-106.9 (1038_16_92_4), #2788
    ('gdp_volume_growth', 'gus_dbw', '%', 50, 200),
    -- WHO data.who.int (#2408): years; highest 87.37 (all areas, 2000-2021, Dev trial 43159213097). Documentation only
    -- until metrics_life_expectancy joins the metrics union below.
    ('life_expectancy', 'who', '%', 0, 130),
    -- UNESCO UIS (#2411): % of GDP; highest 6.35 (all areas, Dev trial 43159213097). Documentation only until
    -- metrics_gerd_gdp joins the metrics union below.
    ('gerd_gdp', 'unesco_uis', '%', 0, 9.5),
    -- UNESCO UIS (#2411): % of population 25+; capped at 100 (all areas max 73.91). Documentation only until
    -- metrics_tertiary_attainment joins the metrics union below.
    ('tertiary_attainment', 'unesco_uis', '%', 0, 100),
    -- FAOSTAT (#2402): published in kt CO2eq, metrics in Mt CO2e since D33 (÷ 1000); World 54,253,565 kt = 54,253.565 Mt
    -- (all sectors without LULUCF, 2024, Dev trial 43159213097). Documentation only until metrics_ghg_emissions joins
    -- the metrics union below.
    ('ghg_emissions', 'faostat', '%', 0, 8.2e4),
    -- FAOSTAT (#2402): published in 1000 ha, metrics in km² since D33 (× 10); World land area 13,040,161.55 thousand ha
    -- = 130,401,615.5 km² (Dev trial 43159213097). Documentation only (not in the metrics union below).
    ('area', 'faostat', '%', 0, 2e8),
    -- World Bank GEM (#2373): CPI % y-o-y, EU27 members 1987-2026: -6.5 to 3,453.3 (1990s hyperinflation; Dev trial
    -- 43283254888). Documentation only until poc_metrics_cpi_annual_rate (it exists) joins the union below.
    ('cpi_annual_rate', 'world_bank_gem', 'world_bank_gem:CPTOTSAXNZGY:pct', -50, 5000),
    -- World Bank GEM (#2373): unemployment rate %, highest 28.7 (Dev trial 43283254888); documentation only (no
    -- metrics model for the monthly KPI yet).
    ('unemployment_rate_monthly_sa', 'world_bank_gem', 'world_bank_gem:UNEMPSA_:pct', 0, 60),
    -- UNHCR (#2415): persons; highest refugees 4,404,995 (all asylum countries 1951-2025, Dev trial 43282453427).
    -- Documentation only (no metrics_refugees_hosted model yet).
    ('refugees_hosted', 'unhcr', '%', 0, 2e7),
    -- EEA GovReg (#2424): published in Gg (kt) CO2e, metrics in Mt CO2e (D33, ÷ 1000); highest member DE 1,253,128 kt
    -- = 1,253.1 Mt (total excl. LULUCF, 1990-2024, Dev trial 43282453427). Documentation only until
    -- metrics_ghg_emissions joins the metrics union below.
    ('ghg_emissions', 'eea', 'eea:govreg_ghg_inventory:gg_co2e:Sectors/Totals_excl', 0, 2e3),
    -- NBP balance of payments (#2780): million EUR, Poland; current account 2026-Q2 -6,225, net IIP -283,248 (Dev trial
    -- 43163012721). Documentation only until metrics_current_account_components and metrics_fdi_inflow join the
    -- metrics union below (draft concepts are outside the test's scope).
    ('current_account_components', 'nbp_bop', '%', -1e6, 1e6),
    ('fdi_inflow', 'nbp_bop', '%', -1e6, 1e6),
    -- percent per year; highest 24.0 in 1998 (silver.nbp_policy_rates, Dev trial 43093114523)
    ('nbp_reference_rate', 'nbp_interest_rates', '%', 0, 30),
    -- slice needs-block rows (#2743): every source of the concepts unioned below
    -- 550.6 (Dev trial 43093114523: a candidate country in the 1990s)
    ('core_inflation', 'eurostat', '%', -10, 600),
    -- national currencies; 322,800 (Dev trial 43093114523)
    ('min_wage', 'eurostat', '%', 1, 1e6),
    ('adult_learning', 'eurostat', '%', 0, 100),
    ('adult_learning', 'gus_bdl', '%', 0, 100),
    ('extreme_poverty', 'gus_bdl', '%', 0, 100),
    -- % change on a year earlier
    ('industrial_production', 'eurostat', '%', -100, 300),
    -- index, previous year = 100
    ('industrial_production', 'gus_bdl', '%', 1, 300),
    -- index, same month a year earlier = 100
    ('industrial_production', 'gus_dbw', '%', 1, 300),
    -- % change on a year earlier; 261 (Dev trial 43093114523)
    ('construction_output', 'eurostat', '%', -100, 400),
    -- index, same month a year earlier = 100
    ('construction_output', 'gus_dbw', '%', 1, 300),
    -- persons; Poland 6,702,934 in 2026-08
    ('employment_enterprise', 'gus_bdl', '%', 1000, 2e7),
    -- PLN; Poland 7,530.45 in 2026-03
    ('median_wage', 'gus_bdl', '%', 500, 1e5),
    ('median_wage', 'gus_dbw', '%', 500, 1e5),
    -- PLN per m2; 2,202 the lowest (Dev trial 43093114523)
    ('price_m2_secondary', 'gus_bdl', '%', 300, 1e5),
    ('price_m2_secondary', 'gus_dbw', '%', 300, 1e5),
    ('arope_rate', 'eurostat', '%', 0, 100),
    ('arope_rate', 'gus_dbw', '%', 0, 100),
    ('long_term_unemployment', 'eurostat', '%', 0, 100),
    ('long_term_unemployment', 'gus_dbw', '%', 0, 100),
    ('registered_unemployed', 'gus_bdl', '%', 0, 5e6),
    ('registered_unemployed', 'gus_dbw', '%', 0, 5e6),
    -- MRPiPS (#2778): persons, Poland and voivodeships, end of month
    ('registered_unemployed', 'mrpips_unemployment', '%', 0, 5e6),
    -- Findex (#2931): published as a share 0-1, metrics in per cent of adults (D33, × 100); all areas 2011-2024:
    -- 0.40 to 100.0 (Dev trial 43310863075). A share converted twice passes 100
    ('account_ownership', 'world_bank_findex', '%', 0, 100),
    -- World Bank IDS (#2931): published in current US$, metrics in million US$ (D33, ÷ 1,000,000); highest is the
    -- low- and middle-income group in 2025: stocks 9,445,018, debt service 1,398,784 (Dev trial 43310863075). A value
    -- left in US$ passes the bound for every economy with more than 30 (10) million US$
    ('external_debt_stocks', 'world_bank_ids', '%', 0, 3e7),
    ('external_debt_service', 'world_bank_ids', '%', 0, 1e7),
    -- ILOSTAT (#2861): per cent of the labour force, 15+, EU27 members, annual and quarterly national series;
    -- highest 28.153 (GR, quarterly), Poland 2025 = 3.018 (Dev trial 43290214779); the other sources' bound
    ('unemployment_rate', 'ilostat', '%', 0, 60),
    -- UN Statistical Yearbook (#2861), EU27 members and World, selected years: GDP in millions of US dollars as
    -- published (World 2023 = 105,943,460, Poland 809,201), GDP per capita in US dollars (highest 128,936 LU, Poland
    -- 20,876), real GDP growth in per cent (-10.9 to 24.6 IE, Poland 2023 = 0.1); Dev trial 43290214779, the WDI bounds
    ('gdp_current_prices', 'undata', '%', 0, 3e8),
    ('gdp_per_capita', 'undata', '%', 0, 5e5),
    ('gdp_volume_growth', 'undata', '%', -100, 200),
    -- Draft KPIs of BIS, UNDP HDR and UNdata (#2861), in the metrics union below; measured in Dev trial 43290214779.
    -- Policy rate, per cent per year: -0.75 to 355,085.64 (BR, hyperinflation, as the BIS publishes it), Poland
    -- 2026-10 = 3.75; the upper bound only catches a broken file.
    ('policy_rate', 'bis', '%', -5, 5e5),
    -- Residential property prices, year-on-year change in per cent: nominal -42.25 to 360.35 (IT, 1940s), real
    -- -45.84 to 59.64 (LT); Poland 2026-Q2 = 6.29 and 3.29.
    ('house_price_growth', 'bis', '%', -100, 1000),
    ('real_house_price_growth', 'bis', '%', -100, 300),
    -- Credit to the private non-financial sector, % of GDP: 7.5 to 482.6 (LU), Poland 2026-Q1 = 57.1.
    ('credit_to_gdp', 'bis', '%', 0, 1000),
    -- Debt service ratio of the private non-financial sector, per cent of income: 3.0 to 40.2 (BR), Poland 6.3.
    ('debt_service_ratio', 'bis', '%', 0, 100),
    -- Human Development Index, 0 to 1 by definition: 0.608 to 0.962 (DK), Poland 2023 = 0.906.
    ('hdi', 'undp_hdr', '%', 0, 1),
    -- Individuals using the internet, % of the population: 15.6 to 99.3 (LU), Poland 2023 = 86.4.
    ('internet_users', 'undata', '%', 0, 100),
    -- Current health expenditure, % of GDP: 4.9 to 12.9 (DE), Poland 2022 = 6.4.
    ('health_expenditure_gdp', 'undata', '%', 0, 30),
    -- The new sources of KPIs whose metrics models are not in the union below (their other sources have no bound
    -- either): documentation until those models join it. Dev trial 43290214779: reer 9.59 to 519.14 (AR; index,
    -- 2020 = 100), employment rate 43.857 to 83.293, activity rate 45.944 to 85.643, NEET 3.079 to 77.747 (BG),
    -- youth unemployment 1.627 to 60.726 (GR), life expectancy 65.11 to 83.72 years, GERD 0.4 to 3.7 % of GDP.
    ('reer', 'bis', '%', 0, 2000),
    ('employment_rate', 'ilostat', '%', 0, 100),
    ('activity_rate', 'ilostat', '%', 0, 100),
    ('neet_rate', 'ilostat', '%', 0, 100),
    ('youth_unemployment', 'ilostat', '%', 0, 100),
    ('life_expectancy', 'undp_hdr', '%', 20, 100),
    ('gerd_gdp', 'undata', '%', 0, 10),
    -- UNECE Statistical Database (#2861, #2885), 56 member countries, EU-27 and euro area; Dev trial 43306220004.
    -- Unemployment rate, annual, per cent: 0.0 (KZ 1991) to 38.8 (MK), Poland 2025 = 3.1. Population: published in
    -- thousands of persons, metrics in persons (D33, x 1000): 29,000 to 451,935,000 (EU-27), Poland 37,389,000.
    -- Industrial production, annual growth in per cent: -36.7 to 39.4 (SK), Poland 2025 = 2.6.
    ('unemployment_rate', 'unece', '%', 0, 80),
    ('population', 'unece', '%', 0, 1e9),
    ('industrial_production', 'unece', '%', -80, 300),
    -- UNECE in KPIs whose metrics models are not in the union below: documentation until those models join it.
    -- CPI growth -8.0 to 20,757.4 (AM, 1990s hyperinflation), PPI growth -46.4 to 7,687.6 (UA); monthly
    -- seasonally adjusted unemployment rate 0.5 to 36.7 (MK).
    ('unemployment_rate_monthly_sa', 'unece', '%', 0, 80),
    ('cpi_annual_rate', 'unece', '%', -50, 1e5),
    ('ppi_annual_rate', 'unece', '%', -80, 1e5)
),

metrics as (
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_population') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_population_projection') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_gdp_current_prices') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_inflation') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_unemployment_rate') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_fx_rate') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_gdp_per_capita') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_gdp_volume_growth') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_nbp_reference_rate') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_core_inflation') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_min_wage') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_adult_learning') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_extreme_poverty') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_industrial_production') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_construction_output') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_employment_enterprise') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_median_wage') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_price_m2_secondary') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_arope_rate') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_long_term_unemployment') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_registered_unemployed') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_museum_visitors') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_cinema_admissions') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_public_library_users') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_tourists_accommodated') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_tourist_overnight_stays') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_tourist_bed_places') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_museum_visiting_share') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_theatre_attendance_share') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_urban_population_share') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_policy_rate') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_account_ownership') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_house_price_growth') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_agricultural_holdings') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_real_house_price_growth') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_external_debt_stocks') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_credit_to_gdp') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_average_farm_area') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_debt_service_ratio') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_external_debt_service') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_hdi') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_social_assistance_recipients') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_internet_users') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_health_expenditure_gdp') }}
    union all
    select
        concept_key,
        source_system,
        measure_key,
        geography_key,
        date_key,
        value
    from {{ ref('poc_metrics_vehicle_registrations') }}
)

select
    m.concept_key,
    m.source_system,
    m.measure_key,
    m.geography_key,
    m.date_key,
    m.value,
    b.value_min,
    b.value_max
from metrics as m
left join bounds as b
    on
        m.concept_key = b.concept_key
        and m.source_system = b.source_system
        and m.measure_key like b.measure_key_pattern
where m.value < b.value_min or m.value > b.value_max or b.value_min is null
