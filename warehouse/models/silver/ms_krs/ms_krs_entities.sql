{# KRS entities from the Open KRS API (Ministry of Justice): one row per KRS number asked for, from the newest
   Landing copy of each part (bronze.ms_krs_extracts). Grain: krs_number. Selection: the KRS numbers GLEIF lists
   (entities with an LEI), loaded over several runs, so this is not the register and its row count is not a
   count of Polish companies. has_current_extract = false: neither register P nor S holds a current extract of
   the number (http_status 204; entities struck off the register), all other columns are then null.
   Entity-level fields only; the entity's registered name can contain a surname (partnerships), no other
   personal data is loaded. Typed: dates from dd.mm.yyyy, share capital from a decimal comma; regon as published
   (14 digits, the 9-digit REGON padded with zeros). main_pkd_code only when the extract names exactly one main
   activity (main_pkd_count says how many). in_liquidation / in_bankruptcy: the current extract carries such an
   entry without an end. #}
with newest as (
    select *
    from {{ source('ms_krs', 'ms_krs_extracts') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    krs_number,
    http_status = '200' as has_current_extract,
    register,
    legal_form,
    name,
    nip,
    regon,
    cast(has_opp_status as boolean) as has_opp_status,
    seat_country,
    seat_voivodeship,
    seat_powiat,
    seat_gmina,
    seat_town,
    cast(try_strptime(registered_on, '%d.%m.%Y') as date) as registered_on,
    cast(try_strptime(last_entry_on, '%d.%m.%Y') as date) as last_entry_on,
    try_cast(last_entry_number as integer) as last_entry_number,
    cast(try_strptime(state_as_of, '%d.%m.%Y') as date) as state_as_of,
    try_cast(replace(share_capital, ',', '.') as decimal(20, 2)) as share_capital,
    share_capital_currency,
    case when json_array_length(main_pkd) = 1 then main_pkd ->> '$[0]' end as main_pkd_code,
    cast(json_array_length(main_pkd) as integer) as main_pkd_count,
    try_cast(liquidation_entries as integer) > try_cast(liquidation_ended as integer) as in_liquidation,
    try_cast(bankruptcy_entries as integer) > try_cast(bankruptcy_ended as integer) as in_bankruptcy,
    try_cast(restructuring_entries as integer) > 0 as has_restructuring_entry,
    cast(dissolution_entry as boolean) as has_dissolution_entry,
    cast(requested_on as date) as requested_on,
    entity,
    source_version,
    raw_sha256
from newest
