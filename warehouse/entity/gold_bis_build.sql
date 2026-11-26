-- Gold build stage of the BIS Data Portal selections, by rule (docs/gold-coverage.md): every row of
-- silver.bis_observations with a value, every selection, series and area, in the common observation shape
-- (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's
-- files as one block. Relations:
--   silver    the data files of silver.bis_observations (selection × series × period)
--   geo_map   gold.map_geography rows of BIS (source_code, geography_key)
--   measure_key = bis:bis_observations:<unit>:<selection>:<series codes>@<FREQ>: the series key as the BIS
--                 publishes it without FREQ and the area dimension (REF_AREA, or BORROWERS_CTY in the
--                 credit dataflows), codes joined by '.' in the dataflow's dimension order. Every other
--                 dimension of a dataflow defines the measure (a real and a nominal rate, a narrow and a
--                 broad basket, credit in % of GDP and in US dollars are different measures), so no series
--                 has a breakdown. The selection is in the key because the dimensions differ by dataflow.
--   unit        = UNIT_MEASURE as published (an attribute, or a dimension in WS_SPP), with the power of ten
--                 of UNIT_MULT when it is not 0 (`XDC*10^9`: billions of national currency); D33, nothing is
--                 rescaled. A series without UNIT_MEASURE has unit `_Z`.
--   date_key    = first day of the period (A, Q, M; a day for D); other periods are not loaded
--   geography   = by the area code through map_geography: ISO 3166 alpha-2 countries and XM euro area; the
--                 BIS aggregates (4T, 5R, 5A, G2, XW) have no Gold area and are kept as _X with their code
--   status      = OBS_STATUS A or none ok, B break, P provisional, E estimated; any other code `flagged`
-- A cell without a number ('NaN', OBS_STATUS M) has no value in Silver and is not loaded; nothing is 0-filled.
with placed as (
    select
        s.*,
        case
            when s.freq = 'A' and regexp_matches(s.time_period, '^\d{4}$')
                then make_date(cast(s.time_period as integer), 1, 1)
            when s.freq = 'Q' and regexp_matches(s.time_period, '^\d{4}-Q[1-4]$')
                then make_date(cast(left(s.time_period, 4) as integer), cast(right(s.time_period, 1) as integer) * 3 - 2, 1)
            when s.freq = 'M' and regexp_matches(s.time_period, '^\d{4}-(0[1-9]|1[0-2])$')
                then cast(s.time_period || '-01' as date)
            when s.freq = 'D' and regexp_matches(s.time_period, '^\d{4}-\d{2}-\d{2}$')
                then try_cast(s.time_period as date)
        end as date_key,
        array_to_string(
            list_transform(
                list_filter(
                    json_keys(s.dimensions_json),
                    lambda k: k <> 'FREQ'
                    and k <> case when json_exists(s.dimensions_json, '$.REF_AREA') then 'REF_AREA' else 'BORROWERS_CTY' end
                ),
                lambda k: json_extract_string(s.dimensions_json, '$.' || k)
            ),
            '.'
        ) as series_codes,
        coalesce(s.unit_measure, '_Z')
        || case when coalesce(s.unit_mult, '0') = '0' then '' else '*10^' || s.unit_mult end as unit
    from silver as s
    where s.obs_value is not null
)

select
    'bis' as source_system,
    'bis_observations' as block_id,
    'BIS Data Portal statistics (policy rates, effective exchange rates, property prices, credit, debt service)'
        as block_name,
    'bis:bis_observations:' || p.unit || ':' || p.entity || ':' || p.series_codes || '@' || p.freq as measure_key,
    'BIS ' || p.dataflow_id || ' — ' || p.series_codes || ' — ' || p.unit || ' [' || p.freq || ']' as measure_name,
    p.entity || ':' || p.series_codes as measure_codes,
    p.unit as unit_code,
    p.unit as unit_name,
    p.ref_area as geography_source_code,
    case when p.ref_area is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    p.freq as frequency,
    p.date_key,
    p.time_period as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    p.obs_value as value,
    case coalesce(p.obs_status, 'A')
        when 'A' then 'ok' when 'B' then 'break' when 'P' then 'provisional' when 'E' then 'estimated'
        else 'flagged'
    end as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
left join geo_map as g on p.ref_area = g.source_code
where p.date_key is not null
