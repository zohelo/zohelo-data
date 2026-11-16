{{ config(alias='build_cie_sio', tags=['gold_block']) }}
{# Gold build stage: SIO archival education statistics of cie.gov.pl (register PLS-022), annual observations at the
   SIO reference date 30 September (date_key = 1 January of that year, period_label keeps the date).
   Blocks: 'teachers' (silver.cie_sio_teachers: persons, full-time, part-time, full-time posts by professional grade;
   the grade total 'Razem' is the total series), 'schools' (silver.cie_sio_schools: schools and pupils 2018 by school
   type, pupil category, publicness and governing body; no published total, none derived), 'graduates'
   (silver.cie_sio_graduates: graduates and of which girls by school type and pupil category). One measure_key per
   measure (frequency A only); areas by GUS voivodeship id or 'PL' (macros/sources/cie_sio.sql). Values and units
   as published; rows without a value are not loaded. #}
with geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'cie_sio'
),

teachers as (
    select
        'teachers' as block_id,
        'SIO teachers: persons and full-time posts by professional grade (30 September)' as block_name,
        'cie_sio:teachers:' || unit_code || ':' || measure_code as measure_key,
        case measure_code
            when 'persons' then 'Teachers, persons (SIO, 30 September)'
            when 'full_time' then 'Teachers employed full-time, persons (SIO, 30 September)'
            when 'part_time' then 'Teachers employed part-time, persons (SIO, 30 September)'
            when 'posts' then 'Teachers, full-time equivalent posts (SIO, 30 September)'
        end as measure_name,
        measure_code as measure_codes,
        unit_code,
        area_code,
        reference_date,
        period_year,
        case
            when grade_code = '_T' then '{}'
            else cast(to_json(map(['grade'], [grade_code])) as varchar)
        end as breakdown_json,
        case
            when grade_code = '_T' then cast(null as varchar)
            else cast(to_json(map(['grade'], [grade_label])) as varchar)
        end as breakdown_label_json,
        cast(null as varchar) as sex_code,
        grade_code = '_T' as is_total,
        value,
        source_version,
        raw_sha256
    from {{ ref('cie_sio_teachers') }}
),

schools as (
    select
        'schools' as block_id,
        'SIO schools and pupils by school type, category, publicness and governing body (30 September)' as block_name,
        'cie_sio:schools:' || unit_code || ':' || measure_code as measure_key,
        case measure_code
            when 'schools' then 'Schools and institutions (SIO, 30 September)'
            when 'pupils' then 'Pupils, pre-school children and students (SIO, 30 September)'
            when 'pupils_preschool_sections' then 'Of which in pre-school sections (SIO, 30 September)'
            when 'pupils_other_type_sections' then 'Of which in sections of another school type (SIO, 30 September)'
        end as measure_name,
        measure_code as measure_codes,
        unit_code,
        area_code,
        make_date(period_year, 9, 30) as reference_date,
        period_year,
        cast(to_json(map(
            ['school_type', 'pupil_category', 'publicness', 'governing_body'],
            [school_type_code, pupil_category_code, publicness, governing_body]
        )) as varchar) as breakdown_json,
        cast(to_json(map(
            ['school_type', 'pupil_category', 'publicness', 'governing_body'],
            [school_type_name, pupil_category, publicness, governing_body]
        )) as varchar) as breakdown_label_json,
        cast(null as varchar) as sex_code,
        false as is_total,
        value,
        source_version,
        raw_sha256
    from {{ ref('cie_sio_schools') }}
),

graduates as (
    select
        'graduates' as block_id,
        'SIO graduates by school type and pupil category (30 September)' as block_name,
        'cie_sio:graduates:persons:graduates' as measure_key,
        'Graduates of the school year ended (SIO, 30 September)' as measure_name,
        'graduates' as measure_codes,
        unit_code,
        area_code,
        make_date(period_year, 9, 30) as reference_date,
        period_year,
        cast(to_json(map(
            ['school_type', 'pupil_category', 'sex'], [school_type_name, pupil_category_code, sex_code]
        )) as varchar) as breakdown_json,
        cast(to_json(map(
            ['school_type', 'pupil_category', 'sex'],
            [school_type_name, pupil_category, case sex_code when 'T' then 'ogółem' else 'w tym dziewczęta' end]
        )) as varchar) as breakdown_label_json,
        sex_code,
        false as is_total,
        value,
        source_version,
        raw_sha256
    from {{ ref('cie_sio_graduates') }}
),

unioned as (
    select * from teachers
    union all
    select * from schools
    union all
    select * from graduates
)

select
    'cie_sio' as source_system,
    u.block_id,
    u.block_name,
    u.measure_key,
    u.measure_name,
    u.measure_codes,
    u.unit_code,
    case u.unit_code
        when 'persons' then 'persons'
        when 'fte' then 'full-time equivalent posts'
        when 'schools' then 'schools and institutions'
    end as unit_name,
    u.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(u.period_year, 1, 1) as date_key,
    strftime(u.reference_date, '%d.%m.%Y') as period_label,
    u.breakdown_json,
    u.breakdown_label_json,
    u.sex_code,
    u.is_total,
    u.value,
    'ok' as observation_status_key,
    u.source_version,
    u.raw_sha256
from unioned as u
left join geo as g on u.area_code = g.source_code
where u.value is not null
