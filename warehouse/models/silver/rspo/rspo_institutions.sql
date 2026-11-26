{# RSPO register of schools and educational institutions, register PLS-021: one row per register entry (school,
   institution, complex or branch) and school year, from the Ministry of National Education's yearly lists on
   dane.gov.pl (dataset 839, state of 30 September; bronze.rspo_institutions, newest copy of each list). Grain:
   rspo_number x school_year. Codes and their labels as published (institution type, unit kind, public status, pupil
   category, specificity, organisational link, governing-body type and class, registering-body type); TERYT codes of
   the entry's gmina, powiat and voivodeship and of its registering body. Counts as published (pupils, classes,
   teachers; null where the list has none, never 0) and full-time equivalents rounded to 4 decimals. No name,
   address, contact, REGON or governing-body identity (Bronze allowlist, ingestion/to_bronze/rspo/load.py). #}
with newest as ({{ poc_rspo_newest('rspo_institutions') }}
)

select
    {{ poc_rspo_text('school_year') }} as school_year,
    cast(left(school_year, 4) as integer) as reference_year,
    make_date(cast(left(school_year, 4) as integer), 9, 30) as reference_date,
    cast({{ poc_rspo_code('rspo_number', 'rspo_institutions.rspo_number') }} as bigint) as rspo_number,
    {{ poc_rspo_code('voivodeship_teryt', 'rspo_institutions.voivodeship_teryt') }} as voivodeship_teryt,
    {{ poc_rspo_code('powiat_teryt', 'rspo_institutions.powiat_teryt') }} as powiat_teryt,
    {{ poc_rspo_code('gmina_teryt', 'rspo_institutions.gmina_teryt') }} as gmina_teryt,
    {{ poc_rspo_text('gmina_type') }} as gmina_type,
    case {{ poc_rspo_text('is_school') }}
        when '1' then true
        when '0' then false
        else error('rspo_institutions.is_school: not 0 or 1: ' || coalesce(is_school, 'NULL'))
    end as is_school,
    {{ poc_rspo_code('main_entity_id', 'rspo_institutions.main_entity_id') }} as main_entity_id,
    {{ poc_rspo_code('parent_entity_id', 'rspo_institutions.parent_entity_id') }} as parent_entity_id,
    {{ poc_rspo_code('parent_type_id', 'rspo_institutions.parent_type_id') }} as parent_type_id,
    {{ poc_rspo_text('parent_type_name') }} as parent_type_name,
    {{ poc_rspo_code('institution_type_id', 'rspo_institutions.institution_type_id') }} as institution_type_id,
    {{ poc_rspo_text('institution_type_name') }} as institution_type_name,
    {{ poc_rspo_code('unit_kind_id', 'rspo_institutions.unit_kind_id') }} as unit_kind_id,
    {{ poc_rspo_text('unit_kind_name') }} as unit_kind_name,
    {{ poc_rspo_code('public_status_id', 'rspo_institutions.public_status_id') }} as public_status_id,
    {{ poc_rspo_text('public_status_name') }} as public_status_name,
    {{ poc_rspo_code('pupil_category_id', 'rspo_institutions.pupil_category_id') }} as pupil_category_id,
    {{ poc_rspo_text('pupil_category_name') }} as pupil_category_name,
    {{ poc_rspo_code('school_specificity_id', 'rspo_institutions.school_specificity_id') }} as school_specificity_id,
    {{ poc_rspo_text('school_specificity_name') }} as school_specificity_name,
    {{ poc_rspo_code('organisational_link_id', 'rspo_institutions.organisational_link_id') }} as organisational_link_id,
    {{ poc_rspo_text('organisational_link_name') }} as organisational_link_name,
    {{ poc_rspo_code('governing_body_type_id', 'rspo_institutions.governing_body_type_id') }} as governing_body_type_id,
    {{ poc_rspo_text('governing_body_type_name') }} as governing_body_type_name,
    {{ poc_rspo_text('governing_body_class') }} as governing_body_class,
    {{ poc_rspo_code('registering_voivodeship_teryt', 'rspo_institutions.registering_voivodeship_teryt') }}
        as registering_voivodeship_teryt,
    {{ poc_rspo_code('registering_powiat_teryt', 'rspo_institutions.registering_powiat_teryt') }}
        as registering_powiat_teryt,
    {{ poc_rspo_code('registering_gmina_teryt', 'rspo_institutions.registering_gmina_teryt') }}
        as registering_gmina_teryt,
    {{ poc_rspo_code('registering_body_type_id', 'rspo_institutions.registering_body_type_id') }}
        as registering_body_type_id,
    {{ poc_rspo_text('registering_body_type_name') }} as registering_body_type_name,
    {{ poc_rspo_count('pupils', 'rspo_institutions.pupils') }} as pupils,
    {{ poc_rspo_count('pupils_girls', 'rspo_institutions.pupils_girls') }} as pupils_girls,
    {{ poc_rspo_count('pupils_preschool_classes', 'rspo_institutions.pupils_preschool_classes') }}
        as pupils_preschool_classes,
    {{ poc_rspo_count('classes', 'rspo_institutions.classes') }} as classes,
    {{ poc_rspo_count('teachers', 'rspo_institutions.teachers') }} as teachers,
    {{ poc_rspo_count('teachers_full_time', 'rspo_institutions.teachers_full_time') }} as teachers_full_time,
    {{ poc_rspo_count('teachers_part_time', 'rspo_institutions.teachers_part_time') }} as teachers_part_time,
    {{ poc_rspo_fte('teacher_fte', 'rspo_institutions.teacher_fte') }} as teacher_fte,
    {{ poc_rspo_fte('teacher_fte_part_time', 'rspo_institutions.teacher_fte_part_time') }} as teacher_fte_part_time,
    entity,
    source_row_number,
    raw_sha256,
    source_version,
    ingested_at
from newest
