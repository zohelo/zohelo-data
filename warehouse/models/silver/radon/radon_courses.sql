{# RAD-on (OPI PIB) study programmes from POL-on (register PLS-023; API resource "courses", in POL-on "studia
   prowadzone na określonym kierunku"): one row per programme record of the newest Landing copy
   (bronze.radon_courses). Grain: course_uuid (POL-on's own id). Codes and names as published (level, profile,
   ISCED, current status); the leading institution and its voivodeship as the record names them (lower case).
   launches = the programme's launches (courseInstances: one per form, title and start), kept as JSON with the
   disciplines; leading_discipline_code only when exactly one discipline is marked leading (none or several: null). #}
with newest as (
    select *
    from {{ source('radon', 'radon_courses') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

disciplines as (
    select
        courseuuid as course_uuid,
        unnest(json_extract_string(disciplines, '$[*].disciplineCode')) as discipline_code,
        unnest(json_extract_string(disciplines, '$[*].disciplineLeading')) as discipline_leading
    from newest
),

leads as (
    select
        course_uuid,
        count(*) as leading_count,
        min(discipline_code) as leading_code
    from disciplines
    where discipline_leading = 'Tak'
    group by course_uuid
)

select
    n.courseuuid as course_uuid,
    n.coursecode as course_code,
    n.courseoldcode as course_old_code,
    n.coursename as course_name,
    n.levelcode as level_code,
    n.levelname as level_name,
    n.profilecode as profile_code,
    n.profilename as profile_name,
    n.iscedcode as isced_code,
    n.iscedname as isced_name,
    n.currentstatuscode as status_code,
    n.currentstatusname as status_name,
    try_cast(n.creationdate as date) as creation_date,
    try_cast(n.liquidationdate as date) as liquidation_date,
    try_cast(n.terminationinitializationdate as date) as termination_initialization_date,
    n.maininstitutionuuid as main_institution_uuid,
    n.maininstitutionname as main_institution_name,
    n.maininstitutionkindcode as main_institution_kind_code,
    n.maininstitutionkind as main_institution_kind,
    n.leadinginstitutionuuid as leading_institution_uuid,
    n.leadinginstitutionname as leading_institution_name,
    n.leadinginstitutionvoivodeshipcode as leading_voivodeship_code,
    lower(n.leadinginstitutionvoivodeship) as leading_voivodeship,
    n.leadinginstitutioncity as leading_city,
    n.leadinginstitutionisforeign = 'Tak' as leading_institution_is_foreign,
    n.supervisinginstitutionuuid as supervising_institution_uuid,
    n.supervisinginstitutionname as supervising_institution_name,
    n.coled = 'Tak' as is_co_led,
    n.teachertraining = 'Tak' as is_teacher_training,
    n.philological = 'Tak' as is_philological,
    n.legalbasistypecode as legal_basis_type_code,
    case when l.leading_count = 1 then l.leading_code end as leading_discipline_code,
    cast(json_array_length(n.courseinstances) as integer) as launches,
    n.disciplines as disciplines_json,
    n.courseinstances as launches_json,
    n.datasource as data_source,
    n.lastrefresh as last_refresh,
    n.source_version,
    n.raw_sha256
from newest as n
left join leads as l on n.courseuuid = l.course_uuid
