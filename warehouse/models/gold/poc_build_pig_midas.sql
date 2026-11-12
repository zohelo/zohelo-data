{{ config(alias='build_pig_midas', tags=['gold_block']) }}
{# Gold build stage (#2870): PIG-PIB MIDAS, an entity snapshot of the newest WFS download (silver.pig_midas_deposits
   and silver.pig_midas_mining_contours, latest-only: one date, the download day). Counts of the current layers only;
   the archival layers are selected archival data, not a stock, and stay in Silver. Measures:
   pig_midas:documented_deposits:deposits = documented deposits (distinct MIDAS deposit numbers; a deposit has one or
   more polygons), breakdown mineral_code (MIDAS code of the main mineral, _T all; a deposit whose polygons carry
   different codes counts under each, so the codes need not add up to _T); pig_midas:mining_areas:mining_areas and
   pig_midas:mining_terrains:mining_terrains = current mining areas and terrains (distinct register numbers; one has
   one or more contours). The layers carry no region code: geography PL (a voivodeship split needs a spatial join to
   PRG boundaries, not done here). #}
with deposits as (
    select
        snapshot_date,
        mineral_code,
        deposit_number,
        mineral_name,
        source_version,
        raw_sha256
    from {{ ref('pig_midas_deposits') }}
    where layer = 'documented'
),

by_mineral as (
    select
        snapshot_date,
        mineral_code,
        count(distinct deposit_number) as n,
        case when count(distinct mineral_name) = 1 then min(mineral_name) end as label,
        max(source_version) as source_version,
        min(raw_sha256) as raw_sha256
    from deposits
    group by snapshot_date, mineral_code
),

deposit_counts as (
    select * from by_mineral
    union all
    select
        snapshot_date,
        '_T' as mineral_code,
        count(distinct deposit_number) as n,
        null as label,
        max(source_version) as source_version,
        min(raw_sha256) as raw_sha256
    from deposits
    group by snapshot_date
),

contour_counts as (
    select
        layer,
        snapshot_date,
        count(distinct register_number) as n,
        max(source_version) as source_version,
        min(raw_sha256) as raw_sha256
    from {{ ref('pig_midas_mining_contours') }}
    where layer in ('mining_areas', 'mining_terrains')
    group by layer, snapshot_date
)

select
    'pig_midas' as source_system,
    'documented_deposits' as block_id,
    'PIG-PIB MIDAS documented mineral deposits' as block_name,
    'pig_midas:documented_deposits:deposits' as measure_key,
    'Documented mineral deposits (MIDAS)' as measure_name,
    'deposits' as measure_codes,
    'deposits' as unit_code,
    'deposits' as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'D' as frequency,
    snapshot_date as date_key,
    cast(snapshot_date as varchar) as period_label,
    cast(json_object('mineral_code', mineral_code) as varchar) as breakdown_json,
    case when label is not null then cast(json_object('mineral_code', label) as varchar) end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    mineral_code = '_T' as is_total,
    cast(n as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from deposit_counts
union all
select
    'pig_midas' as source_system,
    layer as block_id,
    case layer
        when 'mining_areas' then 'PIG-PIB MIDAS mining areas'
        else 'PIG-PIB MIDAS mining terrains'
    end as block_name,
    'pig_midas:' || layer || ':' || layer as measure_key,
    case layer
        when 'mining_areas' then 'Mining areas in force (MIDAS)'
        else 'Mining terrains in force (MIDAS)'
    end as measure_name,
    layer as measure_codes,
    layer as unit_code,
    replace(layer, '_', ' ') as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    'D' as frequency,
    snapshot_date as date_key,
    cast(snapshot_date as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(n as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from contour_counts
