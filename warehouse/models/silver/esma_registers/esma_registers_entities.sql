{# ESMA registers (register PLG-016, ESMA route): the entries of esma_registers_upreg whose home member state is
   Poland - authorised capital-market entities (investment firms, UCITS management companies, alternative investment
   fund managers, trading venues, crowdfunding service providers, ...) and their branches, supervised by KNF - from the
   newest Landing copy of the slice (bronze.esma_registers_entries). Grain: one row per ESMA entry id (`id`, e.g.
   ae600); a branch is its own entry. Codes and texts as published; '' (ESMA's empty field) is null, never a value:
   an empty ae_lei is a missing LEI, never filled from another entry or by name. Dates: ESMA's ISO timestamps
   (always midnight UTC) as dates; the withdrawal date from its ISO field, else from its dd/mm/yyyy text; any other
   shape fails the build. Not landed, so not here: addresses and the free-text comment
   (ingestion/sources/esma_registers.yaml). #}
with newest as (
    select *
    from {{ source('esma_registers', 'esma_registers_entries') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

cleaned as (
    select
        id as entry_id,
        nullif(trim(ae_db_id), '') as db_id_text,
        nullif(trim(ae_entity_type_code), '') as entity_type_code,
        nullif(trim(ae_entity_type_label), '') as entity_type_label,
        nullif(trim(ae_entity_name), '') as entity_name,
        nullif(trim(ae_commercial_name), '') as commercial_name,
        nullif(trim(ae_legalform), '') as legal_form,
        nullif(trim(ae_lei), '') as lei,
        nullif(trim(ae_head_office_lei), '') as head_office_lei,
        nullif(trim(ae_competent_authority), '') as competent_authority,
        nullif(trim(ae_home_member_state), '') as home_member_state,
        nullif(trim(ae_host_member_state), '') as host_member_state,
        nullif(nullif(trim(ae_host_member_states), ''), '[]') as host_member_states,
        nullif(trim(ae_office_type), '') as office_type,
        nullif(trim(ae_status), '') as status,
        nullif(trim(ae_authorisation_notification_date), '') as authorisation_text,
        nullif(trim(ae_authorisation_withdrawal_end_date), '') as withdrawal_text,
        nullif(trim(ae_authorisation_withdrawal_end_date_str), '') as withdrawal_str,
        nullif(trim(ae_last_update), '') as last_update_text,
        nullif(trim(ae_website), '') as website,
        raw_sha256,
        source_version,
        ingested_at
    from newest
)

select
    entry_id,
    case
        when db_id_text is null then null
        when regexp_full_match(db_id_text, '[0-9]+') then cast(db_id_text as bigint)
        else error('esma_registers_entities: ae_db_id is not a whole number: ' || db_id_text)
    end as esma_db_id,
    entity_type_code,
    entity_type_label,
    entity_name,
    commercial_name,
    legal_form,
    lei,
    head_office_lei,
    competent_authority,
    home_member_state,
    host_member_state,
    host_member_states,
    office_type,
    status,
    case
        when authorisation_text is null then null
        when regexp_full_match(authorisation_text, '[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00Z')
            then cast(left(authorisation_text, 10) as date)
        else error('esma_registers_entities: authorisation date not ISO: ' || authorisation_text)
    end as authorisation_date,
    case
        when
            withdrawal_text is not null
            and regexp_full_match(withdrawal_text, '[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00Z')
            then cast(left(withdrawal_text, 10) as date)
        when withdrawal_text is not null
            then error('esma_registers_entities: withdrawal date not ISO: ' || withdrawal_text)
        when withdrawal_str is null then null
        when regexp_full_match(withdrawal_str, '[0-9]{2}/[0-9]{2}/[0-9]{4}')
            then cast(strptime(withdrawal_str, '%d/%m/%Y') as date)
        else error('esma_registers_entities: withdrawal date text not dd/mm/yyyy: ' || withdrawal_str)
    end as withdrawal_date,
    case
        when last_update_text is null then null
        when regexp_full_match(last_update_text, '[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00Z')
            then cast(left(last_update_text, 10) as date)
        else error('esma_registers_entities: ae_last_update not an ISO date: ' || last_update_text)
    end as last_update,
    website,
    raw_sha256,
    source_version,
    ingested_at
from cleaned
