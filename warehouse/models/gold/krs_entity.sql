{{ config(alias='krs_entity') }}
{# Gold entity table (register source, playbook section 7): one row per KRS number with a current extract in the
   Open KRS API (Ministry of Justice), keyed by the register's own id. Selection: the KRS numbers GLEIF lists
   (entities with an LEI), so it joins to GLEIF by the number (silver.gleif_legal_entities:
   registration_authority_id = 'RA000484' and registration_authority_entity_id = krs_number; a few numbers carry
   several LEIs) and to other sources by NIP or REGON, never by name. It is not the whole register.
   Entity-level attributes only; name is the entity's registered name. seat_geography_key = the voivodeship of
   the seat in dim_geography (TERC:<code>), null when the extract names none or a name that is no voivodeship. #}
with entities as (
    select * from {{ ref('ms_krs_entities') }}
    where has_current_extract
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'ms_krs'
)

select
    e.krs_number,
    e.register,
    e.legal_form,
    e.name,
    e.nip,
    e.regon,
    e.seat_voivodeship,
    g.geography_key as seat_geography_key,
    e.seat_powiat,
    e.seat_gmina,
    e.seat_town,
    e.registered_on,
    e.last_entry_on,
    e.share_capital,
    e.share_capital_currency,
    e.main_pkd_code,
    e.in_liquidation,
    e.in_bankruptcy,
    e.has_opp_status,
    e.requested_on,
    e.source_version
from entities as e
left join geo_map as g on e.seat_voivodeship = g.source_code
