-- Gold build stage of the ECB Data Portal selections, by rule (docs/gold-coverage.md): every row of
-- silver.ecb_observations with a value, every selection, series and area, in the common observation shape (the
-- 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's files as one
-- block. Relations:
--   silver    the data files of silver.ecb_observations (selection × series × period, newest copy)
--   geo_map   gold.map_geography rows of ECB (ISO alpha-2 countries; U2, I8, I9, B6, B0, D0 aggregates)
--   measure_key = ecb:ecb_observations:<unit>:<dataflow>:<series codes>@<freq>: the series key as the ECB
--                 publishes it without FREQ and REF_AREA, codes joined by '.' in the dataflow's dimension order
--                 (EXR keeps its currency and its reference currency, BSI its item, data type and suffix …).
--                 Every other dimension defines the measure, as for BIS, so no series has a breakdown.
--   unit        = the UNIT (EST: UNIT_MEASURE) attribute as published, with the power of ten of UNIT_MULT when it
--                 is not 0 (`EUR*10^6`: millions of euro); D33, nothing is rescaled. No unit: `_Z`.
--   frequency   = FREQ; B (business days) is loaded as D, as in the curated stage
--   date_key    = first day of the period (Silver period_start: A, Q, M, D); other periods (W, S, H) are not
--                 loaded
--   geography   = REF_AREA through map_geography; FM, EST and BSI without REF_AREA are the euro area (U2), as in
--                 the curated stage; EXR has no area (_Z); a code Gold has no area for stays _X with its code
--   status      = OBS_STATUS A or none ok, P provisional, E estimated, B break, D definition_differs; any other
--                 code `flagged`
-- Not loaded: a cell without a number ('' or NaN, no value in Silver; nothing is 0-filled) and a value with
-- OBS_STATUS F (forecast): a projection is not an observation.
with placed as (
    select
        s.*,
        array_to_string(
            list_transform(
                list_filter(json_keys(s.dimensions), lambda k: k not in ('FREQ', 'REF_AREA')),
                lambda k: json_extract_string(s.dimensions, '$.' || k)
            ),
            '.'
        ) as series_codes,
        coalesce(s.unit, '_Z')
        || case when coalesce(s.unit_mult, 0) = 0 then '' else '*10^' || cast(s.unit_mult as varchar) end as unit_code,
        case
            when s.ref_area is not null then s.ref_area
            when s.dataflow in ('FM', 'EST', 'BSI') then 'U2'
        end as area_code,
        case when s.freq = 'B' then 'D' else s.freq end as frequency
    from silver as s
    where s.obs_value is not null and not isnan(s.obs_value) and coalesce(s.obs_status, '') <> 'F'
)

select
    'ecb' as source_system,
    'ecb_observations' as block_id,
    'ECB Data Portal selections (exchange rates, key rates, €STR, HICP, yields, balance sheet items)' as block_name,
    'ecb:ecb_observations:' || p.unit_code || ':' || p.dataflow || ':' || p.series_codes || '@' || p.frequency
        as measure_key,
    'ECB ' || p.dataflow || ' — ' || p.series_codes || ' — ' || p.unit_code || ' [' || p.frequency || ']'
        as measure_name,
    p.dataflow || ':' || p.series_codes as measure_codes,
    p.unit_code,
    p.unit_code as unit_name,
    p.area_code as geography_source_code,
    case when p.area_code is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    p.frequency,
    p.period_start as date_key,
    p.time_period as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    p.obs_value as value,
    case coalesce(p.obs_status, 'A')
        when 'A' then 'ok' when 'P' then 'provisional' when 'E' then 'estimated' when 'B' then 'break'
        when 'D' then 'definition_differs'
        else 'flagged'
    end as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join geo_map as g on p.area_code = g.source_code
where p.period_start is not null and p.frequency in ('A', 'Q', 'M', 'D')
