{{ config(alias='build_gios_groundwater', tags=['gold_block']) }}
{# Gold build stage: GIOŚ groundwater quality monitoring (register PL-ENV-012, #2870), block 'nitrate', from
   silver.gios_groundwater_results (nitrate, 'Azotany [mgNO3/l]', inorganic files) and silver.gios_groundwater_points
   (the point's voivodeship that year). Rule: a monitoring point counts in a year when it has at least one nitrate
   result (measured, or below the limit of quantification); it is above 50 mg/l when at least one of its samples of
   the year measured more than 50 mg/l NO3 (the Nitrates Directive threshold). A result below the limit is below 50
   (the Silver test gios_groundwater_results_complete fails on a nitrate limit above 50), so it is never counted as
   above and never read as 0. Measures per monitoring kind (the diagnostic network covers every groundwater body,
   2019 and 2022; the operational network the bodies at risk, the other years; never mixed in one series):
   gios_groundwater:nitrate:monitoring_points:sampled.<kind>, …:above_50.<kind> and
   gios_groundwater:nitrate:%:above_50_share.<kind> (100 x above / sampled). Annual only. Areas: Poland (every point)
   and the voivodeship the result file names for the point (map poc_geography__gios_groundwater, by name: the files
   carry no TERYT code); a name the map does not know goes to _X and is counted by build_check. #}
with nitrate as (
    select *
    from {{ ref('gios_groundwater_results') }}
    where indicator_key = 'azotany[mgno3/l]' and indicator_group = 'inorganic'
),

point_years as (
    select
        n.point_key,
        n.survey_year,
        n.monitoring_kind,
        coalesce(bool_or(n.value_status = 'measured' and n.value > 50), false) as above_50,
        max(n.source_version) as source_version,
        max(n.raw_sha256) as raw_sha256
    from nitrate as n
    group by n.point_key, n.survey_year, n.monitoring_kind
),

geo_map as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gios_groundwater'
),

located as (
    select
        py.*,
        'PL' as geo_code,
        'PL' as geography_key
    from point_years as py
    union all
    select
        py.*,
        p.voivodeship as geo_code,
        coalesce(g.geography_key, '_X') as geography_key
    from point_years as py
    inner join {{ ref('gios_groundwater_points') }} as p
        on py.point_key = p.point_key and py.survey_year = p.survey_year
    left join geo_map as g on p.voivodeship = g.source_code
),

counted as (
    select
        survey_year,
        monitoring_kind,
        geo_code,
        geography_key,
        count(*) as sampled,
        count(*) filter (where above_50) as above,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from located
    group by survey_year, monitoring_kind, geo_code, geography_key
),

measures as (
    select
        c.*,
        m.code,
        m.unit_code,
        case m.code
            when 'sampled' then cast(c.sampled as double)
            when 'above_50' then cast(c.above as double)
            else 100.0 * c.above / c.sampled
        end as value
    from counted as c
    cross join (
        values ('sampled', 'monitoring_points'), ('above_50', 'monitoring_points'), ('above_50_share', '%')
    ) as m (code, unit_code)
)

select
    'gios_groundwater' as source_system,
    'nitrate' as block_id,
    'GIOŚ groundwater monitoring: points above 50 mg/l nitrate' as block_name,
    'gios_groundwater:nitrate:' || unit_code || ':' || code || '.' || monitoring_kind as measure_key,
    case code
        when 'sampled' then 'Groundwater monitoring points with nitrate results'
        when 'above_50' then 'Groundwater monitoring points above 50 mg/l nitrate (NO3)'
        else 'Share of groundwater monitoring points above 50 mg/l nitrate (NO3)'
    end || ' (' || monitoring_kind || ' monitoring)' as measure_name,
    code || '.' || monitoring_kind as measure_codes,
    unit_code,
    case unit_code when '%' then '% of monitoring points' else 'monitoring points' end as unit_name,
    geo_code as geography_source_code,
    geography_key,
    'A' as frequency,
    make_date(survey_year, 1, 1) as date_key,
    cast(survey_year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from measures
