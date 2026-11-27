{{ config(alias='knf_supervised_entity') }}
{# Gold entity table (register source, kpi false; playbook section 7): one row per entry of the ESMA registers whose
   home member state is Poland (register PLG-016, #2994; route 1 of "KNF supervised entities"), keyed by the
   register's own id (entry_id, 'ae<number>'). lei is the LEI the entry carries (null where the register gives none:
   most AIFMs and UCITS management companies); a branch may carry its firm's LEI here or in head_office_lei, so one
   LEI can sit on several entries (a firm's head office, its branches and its other authorisations). The table joins
   GLEIF (gold/silver gleif_legal_entities) by LEI only; entries without an LEI are kept and never matched by name.
   valid_from = the authorisation or notification date, valid_to = the withdrawal date (null while Active). Entity
   type, office type and status as published. No addresses (not requested from the source). #}
select
    entry_id,
    esma_db_id,
    entity_name,
    commercial_name,
    entity_type_code,
    entity_type_label,
    lei,
    head_office_lei,
    office_type,
    status,
    status = 'Active' as is_active,
    authorised_on as valid_from,
    withdrawn_on as valid_to,
    last_updated_on,
    competent_authority,
    host_member_states,
    legal_form,
    funds_count,
    'knf_entities' as source_system,
    source_version,
    raw_sha256
from {{ ref('knf_entities_esma_entries') }}
