{{ config(alias='radon_institution') }}
{# Gold entity table (register source, playbook section 7): one row per institution of the higher education and
   science system in POL-on, from RAD-on (OPI PIB; register PLS-023), keyed by POL-on's own id (institution_uuid).
   Joins other sources by REGON, NIP or KRS number as published, never by name. Every kind and status POL-on lists
   (is_operating = status 'Działająca'). geography_key = the voivodeship in dim_geography (TERC:<code>) by the
   name POL-on prints; null when the record names none (institutions abroad). No natural persons: the head of the
   institution and contact details are dropped before Landing. #}
with institutions as (
    select * from {{ ref('radon_institutions') }}
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'radon'
)

select
    i.institution_uuid,
    i.institution_uid,
    i.polon_id,
    i.name,
    i.regon,
    i.nip,
    i.krs,
    i.kind_code,
    i.kind_name,
    i.university_type_code,
    i.university_type_name,
    i.science_institution_type_code,
    i.science_institution_type_name,
    i.status_code,
    i.status_name,
    i.status_name = 'Działająca' as is_operating,
    i.start_date,
    i.liquidation_start_date,
    i.liquidation_date,
    i.voivodeship,
    g.geography_key,
    i.city,
    i.country,
    i.supervising_institution_uuid,
    i.supervising_institution_name,
    i.source_version,
    i.raw_sha256
from institutions as i
left join geo_map as g on i.voivodeship = g.source_code
