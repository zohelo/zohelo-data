{{ config(alias='build_kowr_markets', tags=['gold_block']) }}
{# Gold build stage: KOWR weekly grain report (register PLS-033), silver.kowr_markets_fob_prices. Block
   'fob_export_prices': FOB export prices in EUR/t as published (D33), one measure_key per commodity x export market
   (frequency D only: the quotation date the table names; one quotation a week). Area of the port: US (Gulf), FR (Rouen,
   Bordeaux); the Black Sea ports are no area of dim_geography and stay '_X' with the source code 'BLACK_SEA'.
   Cells printed "-" are not loaded. The weekly tables (EU purchase prices by member state, commodity exchange offers)
   stay in Silver: curated Gold has no weekly fact (as mrirw_zsrir's weekly tables). #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'kowr_markets'
)

select
    'kowr_markets' as source_system,
    'fob_export_prices' as block_id,
    'KOWR grain export prices FOB (wheat, barley, maize)' as block_name,
    'kowr_markets:fob_export_prices:' || s.unit_code || ':' || s.commodity_code || '.' || s.market_code as measure_key,
    'Export price FOB, ' || case s.commodity_code
        when 'wheat' then 'milling wheat'
        when 'feed_barley' then 'feed barley'
        when 'maize' then 'maize'
    end || ', ' || case s.market_code
        when 'hrw_us_gulf' then 'HRW, US Gulf'
        when 'srw_us_gulf' then 'SRW, US Gulf'
        when 'us_gulf' then 'US Gulf'
        when 'rouen' then 'Rouen'
        when 'bordeaux' then 'Bordeaux'
        when 'black_sea' then 'Black Sea ports'
    end || ' (KOWR from EC data)' as measure_name,
    s.commodity_code || '.' || s.market_code as measure_codes,
    s.unit_code,
    'EUR/t' as unit_name,
    s.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    s.quote_date as date_key,
    strftime(s.quote_date, '%Y-%m-%d') as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(s.value as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('kowr_markets_fob_prices') }} as s
left join geo as g on s.area_code = g.source_code
where s.value is not null
