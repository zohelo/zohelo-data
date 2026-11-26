{{ config(alias='build_mrirw_zsrir', tags=['gold_block']) }}
{# Gold build stage: MRiRW ZSRIR market bulletins (register PLS-032), monthly average purchase prices of
   silver.mrirw_zsrir_prices. Block 'purchase_prices': one measure_key per market x product (frequency M only), unit as
   published (D33: PLN per tonne, per kg live weight, per 100 kg or litres, as the sheet title says); Poland only.
   Cells without a price ('---', 'nld', ...) are not loaded. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'mrirw_zsrir'
)

select
    'mrirw_zsrir' as source_system,
    'purchase_prices' as block_id,
    'MRiRW ZSRIR monthly purchase prices (cereals, raw milk, pigs)' as block_name,
    'mrirw_zsrir:purchase_prices:' || s.unit_code || ':' || s.market || '.' || s.product_code as measure_key,
    'Average purchase price, ' || s.product_name || ' (ZSRIR, ' || s.market || ' bulletin)' as measure_name,
    s.market || '.' || s.product_code as measure_codes,
    s.unit_code,
    s.unit_text as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    s.period_start as date_key,
    strftime(s.period_start, '%Y-%m') as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    s.value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('mrirw_zsrir_prices') }} as s
left join geo as g on g.source_code = 'PL'
where s.value is not null
