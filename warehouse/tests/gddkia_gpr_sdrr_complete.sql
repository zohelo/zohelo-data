-- GDDKiA traffic census GPR (models/silver/gddkia_gpr/gddkia_gpr_sdrr.sql): a column role that stops matching, a file
-- that is not read or a vehicle class put in the wrong column fails here. Rows returned = problems:
--   (1) each of the four tables (2020/21 and 2025, dk and dw) holds at least 2,000 sections (measured: see the YAML);
--   (2) each section has every vehicle class of its edition (2025: total + 8 classes; 2020/21: total + 7, cars and
--       minibuses together) and one status for all of them;
--   (3) on a measured section the classes add up to the published total (at most 1 vehicle per class of rounding).
with per_section as (
    select
        edition_code,
        road_class,
        section_code,
        count(*) as classes,
        count(distinct value_status) as statuses,
        max(value) filter (where vehicle_class = 'motor_vehicles') as total,
        sum(value) filter (where vehicle_class <> 'motor_vehicles') as parts,
        count(*) filter (where vehicle_class <> 'motor_vehicles') as part_classes
    from {{ ref('gddkia_gpr_sdrr') }}
    group by all
),

tables as (
    select * from (values ('2020', 'dk'), ('2020', 'dw'), ('2025', 'dk'), ('2025', 'dw')) as t (edition_code, road_class)
)

select
    t.edition_code,
    t.road_class,
    null as section_code,
    'sections: ' || count(p.section_code) as problem
from tables as t
left join per_section as p on t.edition_code = p.edition_code and t.road_class = p.road_class
group by all
having count(p.section_code) < 2000

union all

select
    edition_code,
    road_class,
    section_code,
    'classes: ' || classes || ', statuses: ' || statuses as problem
from per_section
where classes <> case edition_code when '2025' then 9 else 8 end or statuses <> 1

union all

select
    edition_code,
    road_class,
    section_code,
    'total ' || total || ' <> sum of classes ' || parts as problem
from per_section
where total is not null and abs(total - parts) > part_classes
