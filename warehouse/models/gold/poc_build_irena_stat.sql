{{ config(alias='build_irena_stat', tags=['gold_block']) }}
{# Gold build stage: IRENASTAT electricity statistics (register GL-035), block 'elecstat', from
   silver.irena_stat_electricity. One measure per data type and technology, annual only:
   measure_key irena_stat:elecstat:<unit>:<capacity|generation>.<technology>, e.g.
   irena_stat:elecstat:MW:capacity.total_renewable (installed electrical capacity at the end of the year, a stock) and
   irena_stat:elecstat:GWh:generation.solar_photovoltaic (generation over the year, a flow); the technology part is the
   published label in lower case with every other character run as '_'. The technology is in the measure because
   IRENA publishes no all-technology total: 'Total Renewable' and 'Total Non-Renewable' are parents beside their
   children, and every technology is its own series, never summed or split here (IRENA terms 2.1(viii)). The grid
   connection is the breakdown {"grid_connection": "All" | "On-grid" | "Off-grid"}; 'All' is the total (is_total).
   Values and units as published (D33). Areas through poc_map_geography (section irena_stat, static ISO3); an IRENA
   code without a Gold country maps to _X with its code kept and is counted by build_check (16 codes, 9,466 of 189,929
   rows in Dev trial 45868633684: AIA, BES, BLM, COK, FLK, GLP, GUF, MSR, MTQ, MYT, NIU, REU, SGS, SPM, TKL, TWN). Cells without a value are
   not loaded; a value with a PxWeb status is 'flagged'; a year after the build year cannot be an observation and is left out. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'irena_stat'
),

obs as (
    select
        *,
        trim(lower(regexp_replace(technology, '[^A-Za-z0-9]+', '_', 'g')), '_') as technology_key
    from {{ ref('irena_stat_electricity') }}
    where value is not null
)

select
    'irena_stat' as source_system,
    'elecstat' as block_id,
    'IRENASTAT: electrical capacity and generation by technology' as block_name,
    'irena_stat:elecstat:' || o.unit_code || ':' || o.measure || '.' || o.technology_key as measure_key,
    case o.measure
        when 'capacity' then 'Electrical installed capacity, ' || o.technology || ' (IRENA)'
        else 'Electricity generation, ' || o.technology || ' (IRENA)'
    end as measure_name,
    o.measure || '.' || o.technology_key as measure_codes,
    o.unit_code,
    case o.unit_code when 'MW' then 'megawatts' else 'gigawatt-hours' end as unit_name,
    o.country_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    cast(json_object('grid_connection', o.grid_connection) as varchar) as breakdown_json,
    cast(json_object('grid_connection', o.grid_connection) as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.grid_connection = 'All' as is_total,
    o.value,
    case when o.status is null then 'ok' else 'flagged' end as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
left join geo as g on o.country_code = g.source_code
where o.year <= year(current_date)
