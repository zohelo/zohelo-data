{# The Silver tables read only for a source's own codes are optional (poc_optional_ref): a source before its
   first Bronze load adds no rows here and does not hold back the map and the blocks that read it. The four
   sources that do this (unctad, who, unesco_uis, faostat) call poc_optional_ref in their geography macros;
   ops/repo_check.py pairs those calls with this list, and warehouse/changed.py plans by it. #}
{{ config(
    alias='map_geography',
    meta={'optional_parents': [
        'unctad_fdi_flows_stock', 'unctad_merchandise_trade', 'unctad_services_trade',
        'who_observations', 'unesco_uis_observations', 'faostat_observations',
    ]}
) }}
{# Gold PoC: every source's geography code → the area it means in dim_geography
   (design draft 2: map_geography__<source>, merged into one table). Unmatched codes map to
   '_X' and are counted by the build check. Every source defines poc_geography__<source_system>() in
   warehouse/macros/sources/<source_system>.sql (docs/source-registration.md); the two poc_geography_* calls
   below place them, in name order. #}
with geo as (select * from {{ ref('poc_dim_geography') }}),

{{ poc_geography_ctes() }}
-- Every registered source's CTE, then an empty branch that closes the union (and is all that sqlfluff, which
-- renders no registrations, sees).
mapped as (
    {{ poc_geography_union() }}select
        cast(null as varchar) as source_system,
        cast(null as varchar) as source_code,
        cast(null as varchar) as geography_key
    where false
)

select
    m.source_system,
    m.source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    g.geography_key is not null as matched
from mapped as m
left join geo as g on m.geography_key = g.geography_key and g.geography_key <> '_X'
where m.source_code is not null
qualify row_number() over (partition by m.source_system, m.source_code order by g.geography_key nulls last) = 1
