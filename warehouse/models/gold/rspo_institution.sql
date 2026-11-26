{{ config(alias='rspo_institution') }}
{# Gold entity table (register source, playbook section 7): one row per RSPO number listed in any yearly list
   2020-2025 (register PLS-021), keyed by the register's own number. Attributes as published in the newest list that
   holds the number (the newest version of the entry, chosen by its reference year; the grain of silver.rspo_institutions
   is rspo_number x school_year, so one row qualifies): type, unit kind, public status, pupil category, specificity,
   governing-body type and class, registering-body type, the TERYT codes of the gmina, powiat and voivodeship
   (geography_key = 'TERC:' || gmina code, the key of dim_geography) and of the registering body, the complex it
   belongs to, pupils, classes and teachers of that list. The lists publish no founding, start or liquidation date:
   first_reference_year, last_reference_year and years_listed say in which lists the number appears, and
   in_newest_list is false for an entry missing from the newest list (closed, merged or not reported). No name,
   address, contact, REGON or governing-body identity. #}
with s as (select * from {{ ref('rspo_institutions') }}),

newest as (select max(reference_year) as reference_year from s),

listed as (
    select
        rspo_number,
        min(reference_year) as first_reference_year,
        max(reference_year) as last_reference_year,
        count(*) as years_listed
    from s
    group by rspo_number
)

select
    s.rspo_number,
    'TERC:' || s.gmina_teryt as geography_key,
    s.gmina_teryt,
    s.powiat_teryt,
    s.voivodeship_teryt,
    s.gmina_type,
    s.is_school,
    s.institution_type_id,
    s.institution_type_name,
    s.unit_kind_id,
    s.unit_kind_name,
    s.main_entity_id,
    s.parent_entity_id,
    s.parent_type_id,
    s.parent_type_name,
    s.public_status_id,
    s.public_status_name,
    s.pupil_category_id,
    s.pupil_category_name,
    s.school_specificity_id,
    s.school_specificity_name,
    s.organisational_link_id,
    s.organisational_link_name,
    s.governing_body_type_id,
    s.governing_body_type_name,
    s.governing_body_class,
    s.registering_body_type_id,
    s.registering_body_type_name,
    s.registering_voivodeship_teryt,
    s.registering_powiat_teryt,
    s.registering_gmina_teryt,
    s.pupils,
    s.classes,
    s.teachers,
    s.reference_year,
    s.school_year,
    l.first_reference_year,
    l.last_reference_year,
    l.years_listed,
    l.last_reference_year = n.reference_year as in_newest_list,
    s.source_version,
    s.raw_sha256
from s
inner join listed as l on s.rspo_number = l.rspo_number and s.reference_year = l.last_reference_year
cross join newest as n
