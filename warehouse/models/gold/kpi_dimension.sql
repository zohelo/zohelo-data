select
    cm.concept_key as kpi_key,
    md.dimension as dimension_key,
    cm.measure_key,
    cm.source_system,
    md.members,
    md.member_count,
    md.has_unmatched,
    m.first_date,
    m.last_date
from {{ ref('poc_concept_measures') }} as cm
inner join {{ ref('poc_measure_dimensions') }} as md on cm.measure_key = md.measure_key
inner join {{ ref('poc_dim_measure') }} as m on cm.measure_key = m.measure_key
