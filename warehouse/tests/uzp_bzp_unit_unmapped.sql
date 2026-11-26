-- uzp_bzp: unit codes of gold.build_uzp_bzp (notices and each published currency of a contract value) without a
-- unit_map row of the source, or mapped to a unit_key that unit.csv lacks. The currency comes from the notice form
-- as published (any three capital letters), so a code not seen before can appear on any day. D33: its values stay
-- as published, as their own measure, and are reported here; add the unit, unit_decimals and unit_map rows
-- (warehouse/seed_rows/<list>/uzp_bzp.csv). A warning, as metrics_unit_unmapped: one notice in a new currency
-- must not stop the platform's weekly build of every Gold block.
{{ config(severity='warn') }}

select
    b.unit_code,
    m.unit_key,
    count(*) as build_rows
from {{ ref('poc_build_uzp_bzp') }} as b
left join {{ ref('unit_map') }} as m
    on b.unit_code = m.unit_code and m.source_system = 'uzp_bzp'
left join {{ ref('unit') }} as u
    on m.unit_key = u.unit_key
where m.unit_code is null or u.unit_key is null
group by b.unit_code, m.unit_key
