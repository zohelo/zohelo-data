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
    ('gdp_per_capita', 'eurostat', '%', 0, 2e5),
    ('gdp_per_capita', 'world_bank_wdi', '%', 0, 5e5),
    ('gdp_volume_growth', 'eurostat', '%', -50, 100),
    ('gdp_volume_growth', 'world_bank_wdi', '%', -100, 200),
    -- an index, previous period or same quarter of the previous year = 100 (rate = value - 100);
    -- Poland published 92.6-111.2 (GUS DBW 1038_16_389_3) and 98.0-106.9 (1038_16_92_4), #2788
    ('gdp_volume_growth', 'gus_dbw', '%', 50, 200)
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
