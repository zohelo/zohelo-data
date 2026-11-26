-- Gold build stage of the UNCTAD Data Hub tables, by rule (docs/gold-coverage.md): every measure of every row of
-- the nine Silver tables with a value, every economy and dimension, in the common observation shape (the 21
-- columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to each table's files as one
-- block; the nine tables share one shape (macros/unctad.sql unctad_table), so they share this stage. Relations:
--   silver    the data files of ONE Silver table (period × economy × dimensions; the measures are a JSON map of
--             value, footnote and missing-value reason by measure name)
--   geo_map   gold.map_geography rows of UNCTAD (UNCTAD economy code → area: the EU27 members, 3412 European
--             Union (2020 …) = EU27_2020, 0000 World)
--   block_id    = the Silver table (silver.entity is the Data Hub report code, e.g. US_FdiFlowsStock)
--   measure_key = unctad:<table>:<measure>:<dimension codes>@<freq>: the measure as UNCTAD names it is the unit
--                 (US$ at current prices in millions, percentage of GDP, growth rate, index …), and every other
--                 dimension of the table (flow, direction, category, commodity, port, vessel type …) defines the
--                 measure, codes joined by '.' in the table's column order (`_Z` for an empty code, nothing after
--                 the measure for a table without dimensions). No breakdowns.
--   unit        = the measure name as published; D33, nothing is rescaled
--   date_key    = first day of the period: Year 2025, Quarter 2006Q01, Period (month) 1995M01; a period of any
--                 other form is not loaded. The frequency ends the measure_key, so a measure has one frequency.
--   geography   = the economy code through map_geography, as the curated stage; other economies (countries
--                 outside the EU27, UNCTAD groups) are kept as _X with their code; a table without economies
--                 (commodity prices; liner shipping connectivity, which is by port, a dimension) is _Z
--   status      = from the footnote of the value: estimated (estimate, nowcast), break, provisional or
--                 preliminary; otherwise ok (UNCTAD footnotes are notes, not flags; as the curated stage)
-- Not loaded: a measure without a number, NaN or infinite (nothing is 0-filled); a value that carries a
-- missing-value reason; a value whose footnote says forecast or projection; a period that is not a year, quarter
-- or month.
with cells as (
    select
        s.entity,
        s.period_field,
        s.period,
        s.period_label,
        s.economy,
        s.economy_label,
        cast(s.dimensions as json) as dims,
        cast(s.dimension_labels as json) as dim_labels,
        m.measure,
        try_cast(json_extract_string(s.measure_values, '$."' || m.measure || '"') as double) as value,
        json_extract_string(s.measure_footnotes, '$."' || m.measure || '"') as footnote,
        json_extract_string(s.measure_missing, '$."' || m.measure || '"') as missing,
        s.source_version,
        s.raw_sha256
    from silver as s
    cross join unnest(json_keys(s.measure_values)) as m (measure)
),

placed as (
    select
        c.*,
        case
            when regexp_matches(c.period, '^\d{4}$') then 'A'
            when regexp_matches(c.period, '^\d{4}Q0?[1-4]$') then 'Q'
            when regexp_matches(c.period, '^\d{4}M(0[1-9]|1[0-2])$') then 'M'
        end as frequency,
        case
            when regexp_matches(c.period, '^\d{4}$') then make_date(cast(c.period as integer), 1, 1)
            when regexp_matches(c.period, '^\d{4}Q0?[1-4]$')
                then make_date(cast(left(c.period, 4) as integer), cast(right(c.period, 1) as integer) * 3 - 2, 1)
            when regexp_matches(c.period, '^\d{4}M(0[1-9]|1[0-2])$')
                then make_date(cast(left(c.period, 4) as integer), cast(right(c.period, 2) as integer), 1)
        end as date_key,
        array_to_string(
            list_transform(
                json_keys(c.dims),
                lambda k: coalesce(nullif(json_extract_string(c.dims, '$."' || k || '"'), ''), '_Z')  -- noqa: RF02
            ),
            '.'
        ) as codes,
        array_to_string(
            list_filter(
                list_transform(json_keys(c.dim_labels), lambda k: json_extract_string(c.dim_labels, '$."' || k || '"')),  -- noqa: RF02
                lambda x: coalesce(x, '') <> ''  -- noqa: RF02
            ),
            ', '
        ) as labels
    from cells as c
),

named as (
    select
        p.*,
        case p.entity
            when 'US_FdiFlowsStock' then 'unctad_fdi_flows_stock'
            when 'US_TradeMerchTotal' then 'unctad_merchandise_trade'
            when 'US_TradeServCatTotal' then 'unctad_services_trade'
            when 'US_DigitallyDeliverableServices' then 'unctad_digital_services_trade'
            when 'US_CommodityPrice_M' then 'unctad_commodity_prices'
            when 'US_PLSCI' then 'unctad_liner_shipping_connectivity'
            when 'US_MerchantFleet' then 'unctad_merchant_fleet'
            when 'US_SeaborneTrade' then 'unctad_seaborne_trade'
            when 'US_PortCalls' then 'unctad_port_calls'
        end as table_name
    from placed as p
)

select
    'unctad' as source_system,
    n.table_name as block_id,
    'UNCTAD Data Hub ' || n.entity || ', every measure, economy and dimension' as block_name,
    'unctad:' || n.table_name || ':' || n.measure || case when n.codes = '' then '' else ':' || n.codes end
    || '@' || n.frequency as measure_key,
    coalesce(nullif(n.labels, ''), n.entity) || ' [' || n.measure || ']' as measure_name,
    n.entity || case when n.codes = '' then '' else ':' || n.codes end as measure_codes,
    n.measure as unit_code,
    n.measure as unit_name,
    n.economy as geography_source_code,
    case when n.economy is null then '_Z' else coalesce(g.geography_key, '_X') end as geography_key,
    n.frequency,
    n.date_key,
    coalesce(n.period_label, n.period) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    n.value,
    case
        when n.footnote ilike '%estimat%' or n.footnote ilike '%nowcast%' then 'estimated'
        when n.footnote ilike '%break%' then 'break'
        when n.footnote ilike '%provisional%' or n.footnote ilike '%preliminary%' then 'provisional'
        else 'ok'
    end as observation_status_key,
    n.source_version,
    n.raw_sha256
from named as n
left join geo_map as g on n.economy = g.source_code
where
    n.value is not null
    and not isnan(n.value)
    and not isinf(n.value)
    and n.missing is null
    and n.date_key is not null
    and n.table_name is not null
    and coalesce(n.footnote, '') not ilike '%forecast%'
    and coalesce(n.footnote, '') not ilike '%projection%'
    and coalesce(n.footnote, '') not ilike '%projected%'
