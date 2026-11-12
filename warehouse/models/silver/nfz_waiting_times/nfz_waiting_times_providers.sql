{# NFZ waiting times (register PLS-015): the healthcare providers that list queues in the newest complete monthly
   snapshot, one row per NFZ voivodeship branch and provider code (the code NFZ gives the contracted provider),
   with its name and the TERYT code of its seat as published; the counts of its queue entries and places. Public
   fields only (no REGON, NIP, address or contact data). A provider code listed under two names or seats in one
   branch fails the unique test, it is never resolved by picking one. Grain: province_code x provider_code. #}
select
    province_code,
    provider_code,
    provider_name,
    teryt_provider,
    snapshot_month,
    count(*) as queue_entries,
    count(distinct concat_ws('|', registry_number, resort_part_vii, place_name)) as places
from {{ ref('nfz_waiting_times_queues') }}
group by all
