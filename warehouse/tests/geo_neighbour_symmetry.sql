-- gold.geo_neighbour (#2739): every pair (a, b) has its mirror (b, a) with the same level, shared border and
-- contiguity, so the weight matrices built from it are symmetric. Returns the pairs without a mirror.
select
    a.level_name,
    a.geography_key,
    a.neighbour_key,
    a.shared_border_km
from {{ ref('geo_neighbour') }} as a
left join {{ ref('geo_neighbour') }} as b
    on
        a.geography_key = b.neighbour_key
        and a.neighbour_key = b.geography_key
        and a.level_name = b.level_name
        and a.shared_border_km = b.shared_border_km
        and a.contiguity = b.contiguity
where b.geography_key is null
