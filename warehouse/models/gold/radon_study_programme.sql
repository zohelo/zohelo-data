{{ config(alias='radon_study_programme') }}
{# Gold entity table (register source, playbook section 7): one row per study programme in POL-on (API resource
   "courses"), from RAD-on (OPI PIB; register PLS-023), keyed by POL-on's own id (course_uuid), with the
   institution that leads it (institution_uuid of gold.radon_institution; no relationships test to keep the tables
   independent, the Dev trial counts the misses). Level, profile, ISCED field and status as published;
   geography_key = the voivodeship of the leading institution in dim_geography, null when the record names none. #}
with courses as (
    select * from {{ ref('radon_courses') }}
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'radon'
)

select
    c.course_uuid,
    c.course_code,
    c.course_name,
    c.level_code,
    c.level_name,
    c.profile_code,
    c.profile_name,
    c.isced_code,
    c.isced_name,
    c.status_code,
    c.status_name,
    c.creation_date,
    c.liquidation_date,
    c.main_institution_uuid,
    c.leading_institution_uuid,
    c.leading_institution_name,
    c.leading_voivodeship,
    g.geography_key,
    c.leading_institution_is_foreign,
    c.is_co_led,
    c.is_teacher_training,
    c.leading_discipline_code,
    c.launches,
    c.source_version,
    c.raw_sha256
from courses as c
left join geo_map as g on c.leading_voivodeship = g.source_code
