{{ config(alias='build_gugik_egib', tags=['gold_block']) }}
{# Gold build stage: GUGiK KI EGiB aggregate WFS (register PL-ENV-006), register counts of parcels (block parcels,
   ms:dzialki) and buildings (block buildings, ms:budynki) from silver.gugik_egib_counts. An entity snapshot: one date
   per snapshot = the run date (snapshot_date), frequency D, every snapshot kept. Each count is what the county EGiB
   services supplied to the aggregate service on that day (completeness differs by county), not a census.
   - <block>:<unit>:register_count: a powiat's records (unit code starting with its TERYT code); a voivodeship and
     Poland are the sum of their powiats (labelled so). A powiat answered with 0 (its service supplied nothing that
     day) is no observation and is left out, and the sums that lack it are flagged 'definition_differs'.
   - <block>:<unit>:service_total: Poland only, every record of the service (unfiltered hits).
   - <block>:<unit>:outside_powiat_list: records whose unit code matches none of the 380 powiats: Poland = service
     total - sum of powiats; a voivodeship = its 2-digit prefix count - sum of its powiats. Counts are read minutes
     apart, so a county update between two requests can make it slightly negative; it is kept as computed.
   Lineage: a round is 17 Landing files (parts); a row's raw_sha256 is the file of its part (a powiat's and a
   voivodeship's: the voivodeship part; Poland's: the PL part, the round's first file). #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gugik_egib'
),

counts as (
    select
        *,
        case feature_type when 'dzialki' then 'parcels' else 'buildings' end as block_id
    from {{ ref('gugik_egib_counts') }}
),

powiats as (
    select *
    from counts
    where scope = 'powiat'
),

register_count as (
    select
        block_id,
        snapshot_date,
        area_code,
        record_count as value,
        'ok' as observation_status_key
    from powiats
    where record_count > 0
    union all
    select
        block_id,
        snapshot_date,
        left(area_code, 2) as area_code,
        sum(record_count) as value,
        case when min(record_count) = 0 then 'definition_differs' else 'ok' end as observation_status_key
    from powiats
    group by block_id, snapshot_date, left(area_code, 2)
    union all
    select
        block_id,
        snapshot_date,
        'PL' as area_code,
        sum(record_count) as value,
        case when min(record_count) = 0 then 'definition_differs' else 'ok' end as observation_status_key
    from powiats
    group by block_id, snapshot_date
),

outside as (
    select
        c.block_id,
        c.snapshot_date,
        c.area_code,
        c.record_count - coalesce(sum(p.record_count), 0) as value
    from counts as c
    left join powiats as p
        on
            c.block_id = p.block_id
            and c.snapshot_date = p.snapshot_date
            and (c.scope = 'PL' or left(p.area_code, 2) = c.area_code)
    where c.scope in ('PL', 'voivodeship')
    group by c.block_id, c.snapshot_date, c.area_code, c.record_count
),

measured as (
    select
        *,
        'register_count' as measure_code
    from register_count
    where value > 0
    union all
    select
        block_id,
        snapshot_date,
        area_code,
        record_count as value,
        'ok' as observation_status_key,
        'service_total' as measure_code
    from counts
    where scope = 'PL'
    union all
    select
        *,
        'ok' as observation_status_key,
        'outside_powiat_list' as measure_code
    from outside
),

provenance as (  -- one Landing file per part: PL, or a voivodeship with its powiats
    select
        block_id,
        snapshot_date,
        case scope when 'PL' then 'PL' else left(area_code, 2) end as part,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256,
        count(distinct raw_sha256) as files
    from counts
    group by all
)

select
    'gugik_egib' as source_system,
    m.block_id,
    case m.block_id
        when 'parcels' then 'GUGiK KI EGiB parcels (register count)'
        else 'GUGiK KI EGiB buildings (register count)'
    end as block_name,
    'gugik_egib:' || m.block_id || ':' || m.block_id || ':' || m.measure_code as measure_key,
    case m.block_id when 'parcels' then 'Parcels' else 'Buildings' end || case m.measure_code
        when 'register_count'
            then
                ' in the EGiB register supplied by county services on the day '
                || '(register count, not a census; voivodeship and Poland: sum of powiats)'
        when 'service_total' then ' in the EGiB aggregate service on the day (all records)'
        else ' in the EGiB register with a unit code outside the TERYT powiat list'
    end as measure_name,
    m.measure_code as measure_codes,
    m.block_id as unit_code,
    m.block_id as unit_name,
    m.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'D' as frequency,
    m.snapshot_date as date_key,
    cast(m.snapshot_date as varchar) as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(m.value as double) as value,
    m.observation_status_key,
    p.source_version,
    case
        when p.files > 1
            then error('build_gugik_egib: ' || m.block_id || ' ' || m.snapshot_date || ' part ' || p.part || ' twice')
        else p.raw_sha256
    end as raw_sha256
from measured as m
inner join provenance as p
    on
        m.block_id = p.block_id
        and m.snapshot_date = p.snapshot_date
        and p.part = case m.area_code when 'PL' then 'PL' else left(m.area_code, 2) end
left join geo as g on m.area_code = g.source_code
