{# KNF supervised entities, route 1 (register PLG-016, #2994): one row per entry of the ESMA registers core
   esma_registers_upreg whose home member state is Poland (authorised entities and their branches: AIFMs, investment
   firms, UCITS management companies, markets, crowdfunding providers ...), from the newest landed answer
   (bronze.knf_entities_esma_entries). Grain: entry_id (the ESMA id, 'ae<number>'). Text as published (trimmed; an
   empty string is null). lei and head_office_lei: 20 characters of ISO 17442 or null; a branch may carry its firm's
   LEI in either field, so an LEI repeats across entries and is no key. Dates: the day of the source's
   'YYYY-MM-DDT00:00:00Z' text; any other form fails the build. status: 'Active' or 'Inactive' as published (test).
   funds_count: the number of funds the register lists for an AIFM (no_of_funds), null where the source gives none.
   Not here (not requested from the source): addresses, websites, comments. #}
with newest as (
    select *
    from {{ source('knf_entities', 'knf_entities_esma_entries') }}
    qualify rank() over (order by source_version desc, ingested_at desc) = 1
),

cleaned as (
    select
        nullif(trim(entry_id), '') as entry_id,
        nullif(trim(esma_db_id), '') as esma_db_id_text,
        nullif(trim(entity_name), '') as entity_name,
        nullif(trim(commercial_name), '') as commercial_name,
        nullif(trim(entity_type_code), '') as entity_type_code,
        nullif(trim(entity_type_label), '') as entity_type_label,
        nullif(trim(lei), '') as lei,
        nullif(trim(head_office_lei), '') as head_office_lei,
        nullif(trim(competent_authority), '') as competent_authority,
        nullif(trim(home_member_state), '') as home_member_state,
        nullif(trim(host_member_states), '') as host_member_states,
        nullif(trim(office_type), '') as office_type,
        nullif(trim(status), '') as status,
        nullif(trim(authorisation_date), '') as auth_text,
        nullif(trim(withdrawal_end_date), '') as wd_text,
        nullif(trim(last_update), '') as upd_text,
        nullif(trim(legal_form), '') as legal_form,
        nullif(trim(is_sme), '') as is_sme,
        nullif(trim(funds_count), '') as funds_count_text,
        source_row_number,
        source_version,
        raw_sha256
    from newest
)

select
    entry_id,
    cast(esma_db_id_text as bigint) as esma_db_id,
    entity_name,
    commercial_name,
    entity_type_code,
    entity_type_label,
    case
        when lei is null or regexp_full_match(lei, '[0-9A-Z]{18}[0-9]{2}') then lei
        else error('knf_entities_esma_entries: ae_lei is no LEI: ' || lei)
    end as lei,
    case
        when head_office_lei is null or regexp_full_match(head_office_lei, '[0-9A-Z]{18}[0-9]{2}') then head_office_lei
        else error('knf_entities_esma_entries: ae_headOfficeLei is no LEI: ' || head_office_lei)
    end as head_office_lei,
    competent_authority,
    home_member_state,
    host_member_states,
    office_type,
    status,
    case
        when auth_text is null then null
        when regexp_full_match(auth_text, '\d{4}-\d{2}-\d{2}T00:00:00Z') then cast(left(auth_text, 10) as date)
        else error('knf_entities: unexpected ae_authorisationNotificationDate: ' || auth_text)
    end as authorised_on,
    case
        when wd_text is null then null
        when regexp_full_match(wd_text, '\d{4}-\d{2}-\d{2}T00:00:00Z') then cast(left(wd_text, 10) as date)
        else error('knf_entities: unexpected ae_authorisationWithdrawalEndDate: ' || wd_text)
    end as withdrawn_on,
    case
        when upd_text is null then null
        when regexp_full_match(upd_text, '\d{4}-\d{2}-\d{2}T00:00:00Z') then cast(left(upd_text, 10) as date)
        else error('knf_entities: unexpected ae_lastUpdate: ' || upd_text)
    end as last_updated_on,
    legal_form,
    is_sme,
    case
        when funds_count_text is null then null
        when regexp_full_match(funds_count_text, '\d+') then cast(funds_count_text as integer)
        else error('knf_entities_esma_entries: no_of_funds not a whole number: ' || funds_count_text)
    end as funds_count,
    source_row_number,
    source_version,
    raw_sha256
from cleaned
