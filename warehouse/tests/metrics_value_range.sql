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
    ('gdp_current_prices', 'eurostat', 'eurostat:NAMA_10_GDP:CP_MEUR:B1GQ', 0, 3e7),
    -- million national currency; largest HU 2025 = 87,307,647 million HUF (Eurostat nama_10_gdp, #2788)
    ('gdp_current_prices', 'eurostat', 'eurostat:NAMA_10_GDP:CP_MNAC:B1GQ', 0, 1e9),
    -- million PLN, Poland only; 2025 = 3.915e6 (GUS DBW 1038_16_105_4; Eurostat CP_MNAC PL is the same order)
    ('gdp_current_prices', 'gus_dbw', '%', 0, 3e7),
    ('gdp_current_prices', 'world_bank_wdi', 'world_bank_wdi:NY.GDP.MKTP.CD', 0, 3e14),
    -- current local currency units; largest IR 2025 = 2.693e17 IRR as published (WDI NY.GDP.MKTP.CN, #2788)
    ('gdp_current_prices', 'world_bank_wdi', 'world_bank_wdi:NY.GDP.MKTP.CN', 0, 3e18),
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
    -- FAOSTAT (#2402): kt CO2eq; World 54,253,565 kt (all sectors without LULUCF, 2024, Dev trial 43159213097).
    -- Documentation only until metrics_ghg_emissions joins the metrics union below.
    ('ghg_emissions', 'faostat', '%', 0, 8.2e7),
    -- FAOSTAT (#2402): 1000 ha; World land area 13,040,161.55 (Dev trial 43159213097). Documentation only (no
    -- metrics_area model yet).
    ('area', 'faostat', '%', 0, 2e7),
    -- ILOSTAT (#2861): per cent of the labour force, 15+, EU27 members, annual and quarterly national LFS series;
    -- Poland 2025 = TRIAL, highest value TRIAL (Dev trial TRIAL); the other sources' bound
    ('unemployment_rate', 'ilostat', '%', 0, 60),
    -- UN Statistical Yearbook (#2861), EU27 members and World, selected years: GDP in millions of US dollars as
    -- published (World 2023 = TRIAL, Poland 809,201), GDP per capita in US dollars (highest TRIAL, Poland 20,876),
    -- real GDP growth in per cent (TRIAL to TRIAL; Poland 2023 = 0.1) (Dev trial TRIAL)
    ('gdp_current_prices', 'undata', '%', 0, 3e8),
    ('gdp_per_capita', 'undata', '%', 0, 5e5),
    ('gdp_volume_growth', 'undata', '%', -100, 200)
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
