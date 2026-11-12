-- unit_map.csv (decision D33): a KPI measure whose unit code is matched by two or more unit_map rows with patterns
-- of the same, longest length. The metrics view takes the longest matching measure_key_pattern; a tie would make
-- the source unit depend on row order. Make one pattern more specific or drop one row.
with measures as (
    select distinct
        cm.measure_key,
        cm.source_system,
        dm.unit_key as unit_code
    from {{ ref('poc_concept_measures') }} as cm
    inner join {{ ref('poc_dim_measure') }} as dm on cm.measure_key = dm.measure_key
),

matches as (
    select
        m.measure_key,
        m.source_system,
        m.unit_code,
        um.unit_key,
        coalesce(um.measure_key_pattern, '') as measure_key_pattern,
        length(coalesce(um.measure_key_pattern, '')) as pattern_length
    from measures as m
    inner join {{ ref('unit_map') }} as um
        on
            m.source_system = um.source_system
            and m.unit_code = um.unit_code
            and m.measure_key like coalesce(nullif(um.measure_key_pattern, ''), '%')
),

longest as (
    select
        *,
        max(pattern_length) over (partition by measure_key) as longest_length
    from matches
)

select
    measure_key,
    source_system,
    unit_code,
    list(measure_key_pattern || ' → ' || unit_key order by measure_key_pattern) as tied_rows
from longest
where pattern_length = longest_length
group by measure_key, source_system, unit_code
having count(*) > 1
