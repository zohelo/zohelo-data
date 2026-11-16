{# NFZ waiting times (register PLS-015): the healthcare providers that list queues in the newest complete monthly
   snapshot, one row per NFZ voivodeship branch and provider code (the code NFZ gives the contracted provider), with
   the counts of its queue entries and places (distinct establishment code, resort part VII, and place TERYT code).
   No name, REGON, NIP, registry number, seat or contact data: many providers are one-person practices, and these
   identify the practitioner (they stay only in the Landing bytes). Grain: province_code x provider_code. #}
select
    province_code,
    provider_code,
    snapshot_month,
    count(*) as queue_entries,
    count(distinct concat_ws('|', resort_part_vii, teryt_place)) as places
from {{ ref('nfz_waiting_times_queues') }}
group by all
