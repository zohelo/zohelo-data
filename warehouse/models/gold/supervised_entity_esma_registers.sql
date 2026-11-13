{{ config(alias='supervised_entity_esma_registers') }}
{# Gold entity table (register source, playbook section 7): the entries of the ESMA register (esma_registers_upreg)
   with home member state Poland, one row per entry, keyed by ESMA's own entry id (`ae<number>`; a branch is its own
   entry). The LEI is an attribute: lei as ESMA gives it for the entry, head_office_lei for a branch's head office;
   an empty LEI is null (missing) and is never matched by name or filled from another entry. Validity where ESMA has
   it: valid_from = authorisation or notification date, valid_to = withdrawal date. Texts and codes as published;
   home_geography_key is the home member state through map_geography (PL); host member states stay as ESMA spells
   them (names, not codes, so they are not mapped). No address, no person (docs/sources/esma_registers.md). #}
with e as (select * from {{ ref('esma_registers_entities') }}),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'esma_registers'
)

select
    e.entry_id,
    e.esma_db_id,
    e.lei,
    e.head_office_lei,
    e.entity_name,
    e.commercial_name,
    e.legal_form,
    e.entity_type_code,
    e.entity_type_label,
    e.office_type,
    e.status,
    e.status = 'Active' as is_active,
    e.competent_authority,
    coalesce(g.geography_key, '_X') as home_geography_key,
    e.home_member_state,
    e.host_member_state,
    e.host_member_states,
    e.authorisation_date as valid_from,
    e.withdrawal_date as valid_to,
    e.last_update,
    e.website,
    e.source_version,
    e.raw_sha256
from e
left join geo_map as g on e.home_member_state = g.source_code
