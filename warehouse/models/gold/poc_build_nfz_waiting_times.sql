{{ config(alias='build_nfz_waiting_times', tags=['gold_block']) }}
{# Gold build stage: NFZ waiting times (register PLS-015), block 'waiting_lists', monthly counts of
   silver.nfz_waiting_times_monthly by voivodeship (NFZ codes 01-16 → TERC, poc_geography__nfz_waiting_times) and
   Poland ('00' → PL). One measure_key per count and case (case 1 stable, 2 urgent), frequency M only; date_key =
   the snapshot month (waiting_lists) or the statistics month the counts are published for (persons_awaiting,
   lists_reporting). The counts are zohelo calculations on unchanged published values (measure names say so); the
   forecast time to the benefit is not aggregated. Rows without a value are not loaded. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'nfz_waiting_times'
)

select
    'nfz_waiting_times' as source_system,
    'waiting_lists' as block_id,
    'NFZ waiting lists by voivodeship (monthly snapshots of API Terminy Leczenia)' as block_name,
    'nfz_waiting_times:waiting_lists:' || s.unit_code || ':' || s.measure_code || ':case' || s.case_code as measure_key,
    case s.measure_code
        when 'waiting_lists' then 'NFZ waiting lists (queue entries: provider x place x benefit) listed in the month'
        when 'persons_awaiting' then 'Persons awaiting at month end, sum over NFZ waiting lists (zohelo calculation)'
        when 'lists_reporting' then 'NFZ waiting lists that published a month-end count'
    end || case s.case_code when 1 then ', stable case' when 2 then ', urgent case' end as measure_name,
    s.measure_code || '|case' || s.case_code as measure_codes,
    s.unit_code,
    case s.unit_code when 'persons' then 'persons' when 'waiting_lists' then 'waiting lists' end as unit_name,
    s.province_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'M' as frequency,
    s.period_month as date_key,
    strftime(s.period_month, '%Y-%m') as period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(s.value as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from {{ ref('nfz_waiting_times_monthly') }} as s
left join geo as g on s.province_code = g.source_code
