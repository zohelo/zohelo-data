{{ config(alias='build_ids', tags=['gold_block']) }}
{# Gold build stage (#2819): World Bank International Debt Statistics (silver.ids_observations) in the common
   shape, annual. Series of poc_ids_series: external debt stocks (US$, % of GNI), debt service (US$, % of GNI,
   % of exports). DT.TDS.DECT.GN.ZS has no row in the series member (silver.ids_series in_series_metadata =
   false): its name comes from the observations. measure_key world_bank_ids:<series code>, one frequency (A).
   The debtors are about 120 low- and middle-income economies and their World Bank groups (no Poland, no EU
   member state); the counterpart is WLD for every row, so it is not a breakdown. Debt service is published
   with projections of scheduled payments up to eight years ahead: only years up to the last year of the
   debt stocks (the last actual year) are built, so a projection never becomes a KPI's newest value. #}
with series as ({{ poc_ids_series() }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'world_bank_ids'
),

last_actual as (  -- the last year with actual data: the newest debt-stock year
    select max(year) as year
    from {{ ref('ids_observations') }}
    where series_code = 'DT.DOD.DECT.CD' and value is not null
),

names as (
    select
        series_code,
        series_name
    from {{ ref('ids_series') }}
)

select
    'world_bank_ids' as source_system,
    'IDS_CSV' as block_id,
    'World Bank International Debt Statistics' as block_name,
    'world_bank_ids:' || o.series_code as measure_key,
    coalesce(n.series_name, o.series_name, o.series_code) as measure_name,
    o.series_code as measure_codes,
    s.unit_code,
    case s.unit_code
        when 'usd' then 'current US$' when 'pct_gni' then 'per cent of GNI'
        else 'per cent of exports of goods, services and primary income'
    end as unit_name,
    o.economy_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(o.year, 1, 1) as date_key,
    cast(o.year as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from {{ ref('ids_observations') }} as o
inner join series as s on o.series_code = s.series_code
cross join last_actual as la
left join names as n on o.series_code = n.series_code
left join geo_map as g on o.economy_code = g.source_code
where o.value is not null and o.counterpart_code = 'WLD' and o.year <= la.year
