{{ config(alias='build_gios_surface_water', tags=['gold_block']) }}
{# Gold build stage: GIOŚ surface-water status assessment 2019-2024 (register PL-ENV-011, #3312), block 'status', from
   silver.gios_surface_water_status. Statistical observations, Poland only (the layers carry no voivodeship or other
   area code; a water body crosses voivodeship borders). Per water category (rivers, reservoirs, lakes, transitional,
   coastal, and 'all' = the five together) x assessment (ecological_status, ecological_potential, chemical_status,
   overall_status; monitoring 2019-2024):
   - gios_surface_water:status:water_bodies:<category>.<assessment>: assessed water bodies (is_total, breakdown {}) and
     the bodies per status class (breakdown status_class, the class as the layer publishes it);
   - gios_surface_water:status:%:<category>.<assessment>: the share of each class in the assessed bodies (breakdown
     status_class);
   - gios_surface_water:status:%:<category>.<chemical_status|overall_status>.good: the share of bodies in good status
     (is_total), only where the layer publishes two classes, good and below good; a third class fails the build.
   The overall status with statistical methods (an estimate for unmonitored bodies) stays in Silver. The assessment
   covers six years: one annual row, date_key = 2024-01-01 (the first day of the final year), period_label '2019-2024';
   one frequency per measure. A body counted twice in one category x assessment fails the build. #}
with status as (
    select
        water_category,
        assessment,
        kod_jcwp,
        status_class,
        source_version,
        raw_sha256
    from {{ ref('gios_surface_water_status') }}
    where assessment <> 'overall_status_statistical'
),

assessed as (
    select * from status
    union all
    select
        'all' as water_category,
        assessment,
        kod_jcwp,
        status_class,
        source_version,
        raw_sha256
    from status
),

classes as (
    select
        water_category,
        assessment,
        status_class,
        count(*) as bodies
    from assessed
    group by water_category, assessment, status_class
),

totals as (
    select
        water_category,
        assessment,
        case
            when count(*) <> count(distinct kod_jcwp)
                then error('build_gios_surface_water: a water body twice in ' || water_category || ' ' || assessment)
            else count(*)
        end as bodies,
        count(*) filter (where status_class in ('stan chemiczny dobry', 'dobry stan wód')) as good,
        count(*) filter (where status_class in ('stan chemiczny poniżej dobrego', 'zły stan wód')) as below_good,
        max(source_version) as source_version,
        max(raw_sha256) as raw_sha256
    from assessed
    group by water_category, assessment
),

measures as (
    select
        water_category,
        assessment,
        'water_bodies' as unit_code,
        '' as suffix,
        '{}' as breakdown_json,
        true as is_total,
        cast(bodies as double) as value
    from totals
    union all
    select
        c.water_category,
        c.assessment,
        u.unit_code,
        '' as suffix,
        cast(json_object('status_class', c.status_class) as varchar) as breakdown_json,
        false as is_total,
        case u.unit_code when '%' then 100.0 * c.bodies / t.bodies else cast(c.bodies as double) end as value
    from classes as c
    inner join totals as t on c.water_category = t.water_category and c.assessment = t.assessment
    cross join (values ('water_bodies'), ('%')) as u (unit_code)
    union all
    select
        water_category,
        assessment,
        '%' as unit_code,
        '.good' as suffix,
        '{}' as breakdown_json,
        true as is_total,
        case
            when good + below_good <> bodies then error('build_gios_surface_water: a third class in ' || assessment)
            else 100.0 * good / bodies
        end as value
    from totals
    where assessment in ('chemical_status', 'overall_status')
),

geo_map as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'gios_surface_water'
)

select
    'gios_surface_water' as source_system,
    'status' as block_id,
    'GIOŚ surface water bodies by status class, assessment 2019-2024' as block_name,
    'gios_surface_water:status:' || m.unit_code || ':' || m.water_category || '.' || m.assessment || m.suffix
        as measure_key,
    case m.unit_code when '%' then 'Share of surface water bodies' else 'Surface water bodies' end
    || case when m.suffix = '.good' then ' in good ' else ' by ' end
    || replace(m.assessment, '_', ' ') || ', '
    || case m.water_category
        when 'all' then 'all categories'
        when 'reservoirs' then 'dam reservoirs'
        when 'transitional' then 'transitional waters'
        when 'coastal' then 'coastal waters'
        else m.water_category
    end || ' (GIOŚ monitoring, assessment 2019-2024)' as measure_name,
    m.water_category || '.' || m.assessment || m.suffix as measure_codes,
    m.unit_code,
    case m.unit_code when '%' then '% of assessed water bodies' else 'water bodies' end as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    'A' as frequency,
    date '2024-01-01' as date_key,
    '2019-2024' as period_label,
    m.breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    m.is_total,
    m.value,
    'ok' as observation_status_key,
    t.source_version,
    t.raw_sha256
from measures as m
inner join totals as t on m.water_category = t.water_category and m.assessment = t.assessment
left join geo_map as g on g.source_code = 'PL'
