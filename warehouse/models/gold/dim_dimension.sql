with used as (
    select
        dimension as dimension_key,
        count(distinct measure_key) as measure_count
    from {{ ref('poc_measure_dimensions') }}
    group by dimension
)

select
    coalesce(d.dimension_key, u.dimension_key) as dimension_key,
    coalesce(d.name_en, u.dimension_key) as name_en,
    coalesce(d.name_pl, u.dimension_key) as name_pl,
    coalesce(d.kind, 'source_specific') as kind,
    d.standard,
    coalesce(u.measure_count, 0) as measure_count
from {{ ref('dimension') }} as d
full outer join used as u on d.dimension_key = u.dimension_key
