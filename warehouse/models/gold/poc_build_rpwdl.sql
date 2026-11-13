{{ config(alias='build_rpwdl', tags=['gold_block']) }}
{# Gold build stage (register PLS-020, #2958): RPWDL register counts, an entity snapshot of the newest register
   files (latest-only: one snapshot, no history; Bronze keeps the newest copy of each file). Block 'register', frequency
   A: date_key = 1 January of the snapshot year (the download day of the newest active file, period_label = that
   year); the value is the state on the snapshot day and moves with each weekly load; only the newest snapshot
   year exists (keeping a year-end value per year needs snapshot history, later work). Measures (active entries only,
   Silver is_active): healthcare providers (entities), establishments (facilities), organisational units, beds in
   organisational cells (sum of the published counts; a cell without a count adds nothing) and professional
   practices, the practices also by register (breakdown practice_register, _T = all 8). Areas: voivodeship of the
   TERYT code of the seat (providers), the establishment, unit or cell address, or the practice's correspondence
   address; PL = every active entry, so the voivodeships add up to Poland only when no code is missing (the trial
   reports how many). raw_sha256: the file's SHA-256 when one file feeds the row, else the SHA-256 of the sorted
   list of the files' SHA-256. #}
with entries as (
    select
        'providers' as measure_code,
        'entities' as unit_code,
        voivodeship_code,
        '_T' as practice_register,
        1 as amount,
        snapshot_date,
        source_version,
        raw_sha256
    from {{ ref('rpwdl_providers') }}
    where is_active
    union all
    select
        'establishments' as measure_code,
        'facilities' as unit_code,
        voivodeship_code,
        '_T' as practice_register,
        1 as amount,
        snapshot_date,
        source_version,
        raw_sha256
    from {{ ref('rpwdl_establishments') }}
    where is_active
    union all
    select
        'units' as measure_code,
        'organisational_units' as unit_code,
        voivodeship_code,
        '_T' as practice_register,
        1 as amount,
        snapshot_date,
        source_version,
        raw_sha256
    from {{ ref('rpwdl_units') }}
    where is_active
    union all
    select
        'beds' as measure_code,
        'beds' as unit_code,
        voivodeship_code,
        '_T' as practice_register,
        beds_total as amount,
        snapshot_date,
        source_version,
        raw_sha256
    from {{ ref('rpwdl_cells') }}
    where is_active and beds_total is not null
    union all
    select
        'practices' as measure_code,
        'practices' as unit_code,
        voivodeship_code,
        practice_register,
        1 as amount,
        snapshot_date,
        source_version,
        raw_sha256
    from {{ ref('rpwdl_practices') }}
    where is_active
),

areas as (  -- each entry counted for Poland and for its voivodeship (when the code is one of the 16)
    select
        measure_code,
        unit_code,
        'PL' as area_code,
        practice_register,
        amount,
        snapshot_date,
        source_version,
        raw_sha256
    from entries
    union all
    select
        measure_code,
        unit_code,
        voivodeship_code as area_code,
        practice_register,
        amount,
        snapshot_date,
        source_version,
        raw_sha256
    from entries
    where voivodeship_code is not null
),

cuts as (  -- practices also summed over the registers (_T); the other measures have no breakdown
    select * from areas
    union all
    select
        measure_code,
        unit_code,
        area_code,
        '_T' as practice_register,
        amount,
        snapshot_date,
        source_version,
        raw_sha256
    from areas
    where measure_code = 'practices'
),

snapshot as (  -- one snapshot year per measure, so a series never splits across two years
    select
        measure_code,
        max(snapshot_date) as snapshot_date
    from entries
    group by measure_code
),

counted as (
    select
        measure_code,
        unit_code,
        area_code,
        practice_register,
        sum(amount) as value,
        max(source_version) as source_version,
        case
            when count(distinct raw_sha256) = 1 then min(raw_sha256)
            else sha256(string_agg(distinct raw_sha256, ',' order by raw_sha256))
        end as raw_sha256
    from cuts
    group by measure_code, unit_code, area_code, practice_register
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'rpwdl'
)

select
    'rpwdl' as source_system,
    'register' as block_id,
    'RPWDL register of healthcare providers and practices (counts)' as block_name,
    'rpwdl:register:' || c.unit_code || ':' || c.measure_code as measure_key,
    case c.measure_code
        when 'providers' then 'Healthcare providers in the RPWDL register (active ledgers, snapshot day)'
        when 'establishments' then 'Establishments of healthcare providers in the RPWDL register (snapshot day)'
        when 'units' then 'Organisational units of healthcare providers in the RPWDL register (snapshot day)'
        when 'beds' then 'Beds in organisational cells of healthcare providers, RPWDL register (snapshot day)'
        when 'practices' then 'Professional practices in the RPWDL register (snapshot day)'
    end as measure_name,
    c.measure_code as measure_codes,
    c.unit_code,
    case c.unit_code
        when 'entities' then 'healthcare providers'
        when 'facilities' then 'establishments'
        when 'organisational_units' then 'organisational units'
        when 'beds' then 'beds'
        when 'practices' then 'practices'
    end as unit_name,
    c.area_code as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    make_date(year(s.snapshot_date), 1, 1) as date_key,
    cast(year(s.snapshot_date) as varchar) as period_label,
    case
        when c.measure_code = 'practices' then cast(json_object('practice_register', c.practice_register) as varchar)
        else '{}'
    end as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    c.practice_register = '_T' as is_total,
    cast(c.value as double) as value,
    'ok' as observation_status_key,
    c.source_version,
    c.raw_sha256
from counted as c
inner join snapshot as s on c.measure_code = s.measure_code
left join geo as g on c.area_code = g.source_code
