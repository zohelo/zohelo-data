{{ config(alias='build_radon', tags=['gold_block']) }}
{# Gold build stage: RAD-on / POL-on (OPI PIB; register PLS-023), counts of the register on the day of the newest
   download (frequency D, one date per block; Silver holds the newest copy only, so there is no history yet).
   Block 'institutions': institutions of the higher education and science system with status 'Działająca'
   (operating), by kind (breakdown institution_kind = POL-on's kind code, _T all). Block 'study_programmes': study
   programmes with status X, by level (breakdown level = POL-on's level code, _T all), placed in
   the voivodeship of the leading institution. PL counts every record, also those without a voivodeship; a record
   adds a voivodeship row only when it names one (names that are no voivodeship go to _X). #}
with institutions as (
    select
        'institutions' as block_id,
        'institution_kind' as breakdown,
        kind_code as code,
        kind_name as label,
        voivodeship,
        source_version,
        raw_sha256
    from {{ ref('radon_institutions') }}
    where status_name = 'Działająca'
),

courses as (
    select
        'study_programmes' as block_id,
        'level' as breakdown,
        level_code as code,
        level_name as label,
        leading_voivodeship as voivodeship,
        source_version,
        raw_sha256
    from {{ ref('radon_courses') }}
    where status_code = '3'
),

items as (
    select * from institutions
    union all
    select * from courses
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'radon'
),

located as (
    select
        block_id,
        breakdown,
        coalesce(code, '_U') as code,
        label,
        'PL' as geo_code,
        'PL' as geography_key
    from items
    union all
    select
        i.block_id,
        i.breakdown,
        coalesce(i.code, '_U') as code,
        i.label,
        coalesce(g.source_code, '_X') as geo_code,
        coalesce(g.geography_key, '_X') as geography_key
    from items as i
    left join geo_map as g on i.voivodeship = g.source_code
    where i.voivodeship is not null
),

labels as (
    select
        block_id,
        code,
        max(label) as label,
        count(distinct label) as label_count
    from located
    group by block_id, code
),

counted as (
    select
        block_id,
        breakdown,
        code,
        geo_code,
        geography_key,
        count(*) as n
    from located
    group by block_id, breakdown, code, geo_code, geography_key
    union all
    select
        block_id,
        breakdown,
        '_T' as code,
        geo_code,
        geography_key,
        count(*) as n
    from located
    group by block_id, breakdown, geo_code, geography_key
),

snapshot as (
    select
        block_id,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from items
    group by block_id
)

select
    'radon' as source_system,
    c.block_id,
    case c.block_id
        when 'institutions' then 'RAD-on institutions of higher education and science (POL-on)'
        else 'RAD-on study programmes (POL-on)'
    end as block_name,
    case c.block_id
        when 'institutions' then 'radon:institutions:institutions'
        else 'radon:study_programmes:study_programmes'
    end as measure_key,
    case c.block_id
        when 'institutions' then 'Operating institutions of the higher education and science system (POL-on)'
        else 'Study programmes in operation (POL-on)'
    end as measure_name,
    c.block_id as measure_codes,
    c.block_id as unit_code,
    case c.block_id when 'institutions' then 'institutions' else 'study programmes' end as unit_name,
    c.geo_code as geography_source_code,
    c.geography_key,
    'D' as frequency,
    cast(strptime(s.source_version, '%Y%m%dT%H%M%SZ') as date) as date_key,
    cast(cast(strptime(s.source_version, '%Y%m%dT%H%M%SZ') as date) as varchar) as period_label,
    cast(json_object(c.breakdown, c.code) as varchar) as breakdown_json,
    case
        when c.code <> '_T' and l.label_count = 1 then cast(json_object(c.breakdown, l.label) as varchar)
    end as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.code = '_T' as is_total,
    cast(c.n as double) as value,
    'ok' as observation_status_key,
    s.source_version,
    s.raw_sha256
from counted as c
inner join snapshot as s on c.block_id = s.block_id
left join labels as l on c.block_id = l.block_id and c.code = l.code
