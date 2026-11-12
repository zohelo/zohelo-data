{{ config(alias='build_unctad', tags=['gold_block']) }}
{# Gold build stage: UNCTAD Data Hub measures of poc_unctad_blocks for the EU27 members, the European Union
   (2020 …) group (economy 3412 → EUROSTAT:EU27_2020) and World (0000 → WB:WLD), annual. measure_key
   unctad:<table>:<dimension codes>:<measure>, e.g. unctad:US_FdiFlowsStock:08:1:USD_MN (FDI flow, inward, US$
   million at current prices); the dimension codes are the table's own (FDI Flow 08 flow / 09 stock, Direction
   1 inward / 2 outward; trade Flow 01 imports / 02 exports; services Category S = total services). Economies by
   UNCTAD code (M49; France 250 in the FDI and trade tables), mapped in poc_map_geography (section unctad). Values as
   published. No breakdowns. #}
with blocks as ({{ poc_unctad_blocks() }}),

areas as ({{ poc_unctad_areas() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'unctad'
),

rows_all as (
    select * from {{ ref('unctad_fdi_flows_stock') }}
    union all
    select * from {{ ref('unctad_merchandise_trade') }}
    union all
    select * from {{ ref('unctad_services_trade') }}
    where json_extract_string(dimensions, '$.Category') = 'S'
),

picked as (
    select
        r.*,
        b.block_name,
        b.measure_field,
        b.measure_suffix,
        b.unit_code,
        b.unit_name,
        b.dimension_codes,
        try_cast(json_extract(r.measure_values, '$."' || b.measure_field || '"') as double) as value
    from rows_all as r
    inner join areas as a on r.economy = a.economy
    inner join blocks as b
        on
            r.entity = b.entity
            and b.dimension_codes = array_to_string(
                list_transform(
                    string_split(b.dimension_names, ','),
                    lambda x: json_extract_string(r.dimensions, '$.' || x)  -- noqa: RF02
                ),
                ':'
            )
)

select
    'unctad' as source_system,
    p.entity as block_id,
    p.block_name,
    'unctad:' || p.entity || ':' || p.dimension_codes || ':' || p.measure_suffix as measure_key,
    p.block_name as measure_name,
    p.dimension_codes || ':' || p.measure_suffix as measure_codes,
    p.unit_code,
    p.unit_name,
    p.economy as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(cast(p.period as integer), 1, 1) as date_key,
    p.period as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    p.value,
    case
        when json_extract_string(p.measure_footnotes, '$."' || p.measure_field || '"') ilike '%estimated%'
            then 'estimated'
        when json_extract_string(p.measure_footnotes, '$."' || p.measure_field || '"') ilike 'break%' then 'break'
        else 'ok'
    end as observation_status_key,
    p.source_version,
    p.raw_sha256
from picked as p
left join geo_map as g on p.economy = g.source_code
where p.value is not null
