{# RAD-on (OPI PIB) study programmes from POL-on (register PLS-023; API resource "courses", in POL-on "studia
   prowadzone na określonym kierunku"): one row per programme record of the newest Landing copy
   (bronze.radon_courses). Grain: course_uuid (POL-on's own id). Codes and names as published (level, profile,
   ISCED, current status); the leading institution and its voivodeship as the record names them (lower case).
   launches = the programme's launches (courseInstances: one per form, title and start), kept as JSON with the
   disciplines; leading_discipline_code only when exactly one discipline is marked leading. #}
with newest as (
    select *
    from {{ source('radon', 'radon_courses') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
)

select
    courseuuid as course_uuid,
    coursecode as course_code,
    courseoldcode as course_old_code,
    coursename as course_name,
    levelcode as level_code,
    levelname as level_name,
    profilecode as profile_code,
    profilename as profile_name,
    iscedcode as isced_code,
    iscedname as isced_name,
    currentstatuscode as status_code,
    currentstatusname as status_name,
    try_cast(creationdate as date) as creation_date,
    try_cast(liquidationdate as date) as liquidation_date,
    try_cast(terminationinitializationdate as date) as termination_initialization_date,
    maininstitutionuuid as main_institution_uuid,
    maininstitutionname as main_institution_name,
    maininstitutionkindcode as main_institution_kind_code,
    maininstitutionkind as main_institution_kind,
    leadinginstitutionuuid as leading_institution_uuid,
    leadinginstitutionname as leading_institution_name,
    leadinginstitutionvoivodeshipcode as leading_voivodeship_code,
    lower(leadinginstitutionvoivodeship) as leading_voivodeship,
    leadinginstitutioncity as leading_city,
    leadinginstitutionisforeign = 'Tak' as leading_institution_is_foreign,
    supervisinginstitutionuuid as supervising_institution_uuid,
    supervisinginstitutionname as supervising_institution_name,
    coled = 'Tak' as is_co_led,
    teachertraining = 'Tak' as is_teacher_training,
    philological = 'Tak' as is_philological,
    legalbasistypecode as legal_basis_type_code,
    case
        when list_position(lead.flags, 'Tak') > 0 and list_position(lead.flags, 'Tak') = lead.last_tak
            then json_extract_string(n.disciplines, '$[*].disciplineCode')[list_position(lead.flags, 'Tak')]
    end as leading_discipline_code,
    cast(json_array_length(courseinstances) as integer) as launches,
    disciplines as disciplines_json,
    courseinstances as launches_json,
    datasource as data_source,
    lastrefresh as last_refresh,
    source_version,
    raw_sha256
from newest as n
cross join lateral (
    select
        json_extract_string(n.disciplines, '$[*].disciplineLeading') as flags,
        len(json_extract_string(n.disciplines, '$[*].disciplineLeading'))
        + 1
        - list_position(list_reverse(json_extract_string(n.disciplines, '$[*].disciplineLeading')), 'Tak') as last_tak
) as lead
