-- Gold build stage of the UN World Population Prospects 2024, demographic indicators (medium variant), by rule
-- (docs/gold-coverage.md): every estimate of silver.un_wpp_demographic_indicators, all 54 indicators and every
-- location (countries, areas and the UN's groups), in the common observation shape (the 21 columns of
-- poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's files as one block.
-- Relations:
--   silver      the data files of silver.un_wpp_demographic_indicators (location × year, one column per indicator)
--   geo_map     gold.map_geography rows of UN WPP (countries by ISO two-letter code; the groups European Union
--               (EU: 27) and World by LocID, poc_geography__un_wpp)
--   measure_key = un_wpp:un_wpp_demographic_indicators:<unit>:<indicator>@A: an indicator (the file's column name:
--                 TPopulation1July, TPopulationMale1July, LExFemale …) is one measure whole, as WDI's codes: WPP
--                 publishes the values by sex as indicators of their own, so no series has a breakdown.
--   unit        = as WPP's definitions publish it (D33; nothing is rescaled): persons, births, deaths and migrants
--                 in thousands; density per km²; sex ratios per 100 females (at birth: per 100 female births);
--                 crude rates, rates of natural change and net migration per 1,000 population; growth in per cent;
--                 the infant and under-five rates per 1,000 live births; Q1550 and Q1560 per 1,000 alive at 15;
--                 TFR live births per woman, NRR surviving daughters per woman; ages, life expectancies and the
--                 doubling time in years
--   date_key    = 1 January of the year (the 1 January and 1 July populations are different indicators, so the
--                 reference day stays in the measure); period_label the year
--   geography   = ISO two-letter code where the location has one, else the LocID, through map_geography; a
--                 location Gold has no area for (the UN's regions, income and SDG groups) stays _X with its code
--   status      = `ok`: the file has no flags per cell
-- Not loaded: the projections (is_projection: years 2024-2101 of the 2024 revision; projections are not
-- observations, and a year after the year of the revision is one whatever its flag), an empty cell (no value in
-- Silver; nothing is 0-filled) and a value that is not a finite number.
-- Left out, not loaded: silver.un_wpp_population_age_groups: its columns are sums and ratios Silver computes from
-- the published five-year age groups (those stay in Bronze), not series WPP publishes; the curated stage
-- poc_build_un_wpp keeps the old-age dependency ratio for the KPI layer.
with indicators (indicator, column_name, unit_code, unit_name) as (
    values
    ('TPopulation1Jan', 'tpopulation1jan', 'thousand_persons', 'thousands of persons'),
    ('TPopulation1July', 'tpopulation1july', 'thousand_persons', 'thousands of persons'),
    ('TPopulationMale1July', 'tpopulationmale1july', 'thousand_persons', 'thousands of persons'),
    ('TPopulationFemale1July', 'tpopulationfemale1july', 'thousand_persons', 'thousands of persons'),
    ('PopDensity', 'popdensity', 'persons_per_km2', 'persons per square kilometre'),
    ('PopSexRatio', 'popsexratio', 'males_per_100_females', 'males per 100 females'),
    ('MedianAgePop', 'medianagepop', 'years', 'years'),
    ('NatChange', 'natchange', 'thousand_persons', 'thousands of persons'),
    ('NatChangeRT', 'natchangert', 'per_1000_population', 'per 1,000 population'),
    ('PopChange', 'popchange', 'thousand_persons', 'thousands of persons'),
    ('PopGrowthRate', 'popgrowthrate', 'pct', 'per cent'),
    ('DoublingTime', 'doublingtime', 'years', 'years'),
    ('Births', 'births', 'thousand_births', 'thousands of live births'),
    ('Births1519', 'births1519', 'thousand_births', 'thousands of live births'),
    ('CBR', 'cbr', 'per_1000_population', 'births per 1,000 population'),
    ('TFR', 'tfr', 'children_per_woman', 'live births per woman'),
    ('NRR', 'nrr', 'daughters_per_woman', 'surviving daughters per woman'),
    ('MAC', 'mac', 'years', 'years'),
    ('SRB', 'srb', 'males_per_100_female_births', 'males per 100 female births'),
    ('Deaths', 'deaths', 'thousand_deaths', 'thousands of deaths'),
    ('DeathsMale', 'deathsmale', 'thousand_deaths', 'thousands of deaths'),
    ('DeathsFemale', 'deathsfemale', 'thousand_deaths', 'thousands of deaths'),
    ('CDR', 'cdr', 'per_1000_population', 'deaths per 1,000 population'),
    ('LEx', 'lex', 'years', 'years'),
    ('LExMale', 'lexmale', 'years', 'years'),
    ('LExFemale', 'lexfemale', 'years', 'years'),
    ('LE15', 'le15', 'years', 'years'),
    ('LE15Male', 'le15male', 'years', 'years'),
    ('LE15Female', 'le15female', 'years', 'years'),
    ('LE65', 'le65', 'years', 'years'),
    ('LE65Male', 'le65male', 'years', 'years'),
    ('LE65Female', 'le65female', 'years', 'years'),
    ('LE80', 'le80', 'years', 'years'),
    ('LE80Male', 'le80male', 'years', 'years'),
    ('LE80Female', 'le80female', 'years', 'years'),
    ('InfantDeaths', 'infantdeaths', 'thousand_deaths', 'thousands of deaths'),
    ('IMR', 'imr', 'per_1000_live_births', 'infant deaths per 1,000 live births'),
    ('LBsurvivingAge1', 'lbsurvivingage1', 'thousand_persons', 'thousands of persons'),
    ('Under5Deaths', 'under5deaths', 'thousand_deaths', 'thousands of deaths'),
    ('Q5', 'q5', 'per_1000_live_births', 'deaths under age 5 per 1,000 live births'),
    ('Q0040', 'q0040', 'per_1000_live_births', 'deaths under age 40 per 1,000 live births'),
    ('Q0040Male', 'q0040male', 'per_1000_live_births', 'deaths under age 40 per 1,000 live births'),
    ('Q0040Female', 'q0040female', 'per_1000_live_births', 'deaths under age 40 per 1,000 live births'),
    ('Q0060', 'q0060', 'per_1000_live_births', 'deaths under age 60 per 1,000 live births'),
    ('Q0060Male', 'q0060male', 'per_1000_live_births', 'deaths under age 60 per 1,000 live births'),
    ('Q0060Female', 'q0060female', 'per_1000_live_births', 'deaths under age 60 per 1,000 live births'),
    ('Q1550', 'q1550', 'per_1000_alive_at_15', 'deaths between age 15 and 50 per 1,000 alive at age 15'),
    ('Q1550Male', 'q1550male', 'per_1000_alive_at_15', 'deaths between age 15 and 50 per 1,000 alive at age 15'),
    ('Q1550Female', 'q1550female', 'per_1000_alive_at_15', 'deaths between age 15 and 50 per 1,000 alive at age 15'),
    ('Q1560', 'q1560', 'per_1000_alive_at_15', 'deaths between age 15 and 60 per 1,000 alive at age 15'),
    ('Q1560Male', 'q1560male', 'per_1000_alive_at_15', 'deaths between age 15 and 60 per 1,000 alive at age 15'),
    ('Q1560Female', 'q1560female', 'per_1000_alive_at_15', 'deaths between age 15 and 60 per 1,000 alive at age 15'),
    ('NetMigrations', 'netmigrations', 'thousand_persons', 'thousands of persons'),
    ('CNMR', 'cnmr', 'per_1000_population', 'net migrants per 1,000 population')
),

long as (
    unpivot (
        select * exclude (variant, ingested_at)
        from silver
        where not is_projection
    )
    on columns(* exclude (loc_id, location, iso2, iso3, loc_type, year, is_projection, raw_sha256, source_version))
    into name column_name value value
),

placed as (
    select
        l.*,
        coalesce(l.iso2, cast(l.loc_id as varchar)) as area_code,
        i.indicator,
        i.unit_code,
        i.unit_name
    from long as l
    -- a column of no indicator above has no unit and so no measure_key: the block is rejected, never cut silently
    left join indicators as i on l.column_name = i.column_name
    where
        l.value is not null
        and not isnan(l.value)
        and not isinf(l.value)
        and l.year between 1800 and 2200
)

select
    'un_wpp' as source_system,
    'un_wpp_demographic_indicators' as block_id,
    'UN World Population Prospects 2024, demographic indicators, every location (estimates)' as block_name,
    'un_wpp:un_wpp_demographic_indicators:' || p.unit_code || ':' || p.indicator || '@A' as measure_key,
    p.indicator as measure_name,
    p.indicator as measure_codes,
    p.unit_code,
    p.unit_name,
    p.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(p.year, 1, 1) as date_key,
    cast(p.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    p.value,
    'ok' as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join geo_map as g on p.area_code = cast(g.source_code as varchar)
