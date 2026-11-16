{{ config(alias='build_sejm', tags=['gold_block']) }}
{# Gold build stage: event counts of the Sejm of the Republic of Poland, terms 9 and 10 (from 12 Nov 2019; register
   PLG-024), Poland, per month (M, one frequency per measure_key):
   - block sitting_days: held sitting days (silver.sejm_sitting_days, is_held), sejm:sitting_days:days:monthly;
   - block votings: votings by the day they were held (silver.sejm_votings), sejm:votings:votings:monthly;
   - block prints: prints by their delivery date (silver.sejm_prints; a print without one is not counted, the Dev
     trial counts them), sejm:prints:prints:monthly.
   A month gets a row only when something happened in it: no row is a 0. The month holding the download date of the
   newest answer is provisional. raw_sha256 of a count is the SHA-256 of the sorted Landing hashes it was counted
   from. #}
with events as (
    select
        'sitting_days' as block_id,
        'days' as unit_code,
        day_date as event_date,
        source_version,
        raw_sha256
    from {{ ref('sejm_sitting_days') }}
    where is_held
    union all
    select
        'votings' as block_id,
        'votings' as unit_code,
        voting_date as event_date,
        source_version,
        raw_sha256
    from {{ ref('sejm_votings') }}
    union all
    select
        'prints' as block_id,
        'prints' as unit_code,
        delivery_date as event_date,
        source_version,
        raw_sha256
    from {{ ref('sejm_prints') }}
    where delivery_date is not null
),

snapshot as (
    select cast(strptime(max(source_version), '%Y%m%dT%H%M%SZ') as date) as snapshot_date
    from events
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'sejm'
),

counted as (
    select
        block_id,
        unit_code,
        cast(date_trunc('month', event_date) as date) as date_key,
        strftime(date_trunc('month', event_date), '%Y-%m') as period_label,
        count(*) as events,
        max(source_version) as source_version,
        sha256(string_agg(distinct raw_sha256, '|' order by raw_sha256)) as raw_sha256
    from events
    group by all
)

select
    'sejm' as source_system,
    c.block_id,
    case c.block_id
        when 'sitting_days' then 'Sejm sitting days'
        when 'votings' then 'Sejm votings'
        else 'Sejm prints'
    end as block_name,
    'sejm:' || c.block_id || ':' || c.unit_code || ':monthly' as measure_key,
    case c.block_id
        when 'sitting_days' then 'Sejm sitting days per month'
        when 'votings' then 'Votings held in the Sejm per month'
        else 'Prints delivered to the Sejm per month'
    end || ' (Sejm of the Republic of Poland, terms 9 and 10)' as measure_name,
    'monthly' as measure_codes,
    c.unit_code,
    case c.unit_code when 'days' then 'sitting days' else c.unit_code end as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    c.date_key,
    c.period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(c.events as double) as value,
    case
        when c.date_key = cast(date_trunc('month', s.snapshot_date) as date) then 'provisional'
        else 'ok'
    end as observation_status_key,
    c.source_version,
    c.raw_sha256
from counted as c
cross join snapshot as s
left join geo_map as g on g.source_code = 'PL'
