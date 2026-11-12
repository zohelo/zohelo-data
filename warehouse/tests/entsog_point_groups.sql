{{ config(severity='warn') }}
-- entsog: every point-direction that publishes a physical flow is in the reviewed group list
-- (macros/sources/entsog.sql, poc_entsog_point_groups). An unlisted point goes to the Gold group 'unmapped' (it
-- still counts in the entry and exit totals); this warning names it so the list is extended with a reviewed group.
with groups as ({{ poc_entsog_point_groups() }})

select
    p.operator_key,
    p.point_key,
    p.direction_key,
    p.point_label,
    p.point_type,
    p.first_gas_day,
    p.last_gas_day
from {{ ref('entsog_points') }} as p
left join groups as g on p.point_key = g.point_key
where p.reports_flows and g.point_key is null
