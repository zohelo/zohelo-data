-- KG PSP standard tables (models/silver/kgpsp_interventions/kgpsp_interventions_counts.sql): a header the model no
-- longer reads, a year left out or a column put under the wrong code fails here. Rows returned = problems:
--   (1) every year from 2016 to the newest has both tables;
--   (2) every area (code and name) of a table has all its measures (table 1: 16, table 10: 18) and nothing else;
--   (3) the voivodeship layout has Poland and the 16 voivodeships, the gmina layout at least 2,400 gminas in all 16
--       voivodeships, and each gmina's printed voivodeship is the one its TERYT code names;
--   (4) table 1 adds up where every part is a number: the sizes to their type, P + MZ + AF to OGÓŁEM.
with t as (
    select * from {{ ref('kgpsp_interventions_counts') }}
),

years as (
    select
        y.report_year,
        tc.table_code
    from range(2016, (select max(report_year) + 1 from t)) as y (report_year)
    cross join (values ('1'), ('10')) as tc (table_code)
),

per_area as (
    select
        report_year,
        table_code,
        layout,
        area_code,
        area_name,
        count(distinct measure_code) as measures,
        count(*) as cells
    from t
    group by all
),

per_table as (
    select
        report_year,
        table_code,
        layout,
        count(*) filter (where area_code = 'PL') as poland,
        count(*) filter (where area_code <> 'PL') as areas,
        count(distinct voivodeship_code) filter (where area_code <> 'PL') as voivodeships
    from (select distinct report_year, table_code, layout, area_code, voivodeship_code from t)
    group by all
),

names as (
    select *
    from (
        values
        ('02', 'dolnośląskie'), ('04', 'kujawsko-pomorskie'), ('06', 'lubelskie'), ('08', 'lubuskie'),
        ('10', 'łódzkie'), ('12', 'małopolskie'), ('14', 'mazowieckie'), ('16', 'opolskie'),
        ('18', 'podkarpackie'), ('20', 'podlaskie'), ('22', 'pomorskie'), ('24', 'śląskie'),
        ('26', 'świętokrzyskie'), ('28', 'warmińsko-mazurskie'), ('30', 'wielkopolskie'),
        ('32', 'zachodniopomorskie')
    ) as v (voivodeship_code, voivodeship_name)
),

sums as (
    select
        report_year,
        area_code,
        area_name,
        max(value) filter (where measure_code = 'P') as p,
        sum(value) filter (where measure_code in ('P/M', 'P/Ś', 'P/D', 'P/BD')) as p_parts,
        max(value) filter (where measure_code = 'MZ') as mz,
        sum(value) filter (where measure_code in ('MZ/M', 'MZ/L', 'MZ/Ś', 'MZ/D', 'MZ/K')) as mz_parts,
        max(value) filter (where measure_code = 'AF') as af,
        sum(value) filter (where measure_code in ('A/Z', 'A/DW', 'A/IW')) as af_parts,
        max(value) filter (where measure_code = 'OGÓŁEM') as total,
        count(*) filter (where value_status <> 'ok') as not_numbers
    from t
    where table_code = '1'
    group by all
)

select y.report_year, y.table_code, 'table missing' as problem
from years as y
left join (select distinct report_year, table_code from t) as h
    on y.report_year = h.report_year and y.table_code = h.table_code
where h.report_year is null
union all
select report_year, table_code, 'area ' || area_code || ' ' || area_name || ': ' || measures || ' measures in ' || cells || ' cells'
from per_area
where measures <> (case table_code when '1' then 16 else 18 end) or cells <> measures
union all
select report_year, table_code, layout || ': Poland ' || poland || ', areas ' || areas || ', voivodeships ' || voivodeships
from per_table
where
    (layout = 'voivodeship' and (poland <> 1 or areas <> 16 or voivodeships <> 16))
    or (layout = 'gmina' and (poland <> 0 or areas < 2400 or voivodeships <> 16))
union all
select distinct t.report_year, t.table_code, 'gmina ' || t.area_code || ' printed in ' || coalesce(t.voivodeship_name, '?')
from t
left join names as n on t.voivodeship_code = n.voivodeship_code
where t.layout = 'gmina' and t.voivodeship_name is distinct from n.voivodeship_name
union all
select report_year, '1', 'area ' || area_code || ' ' || area_name || ' does not add up: ' || concat_ws(' ', p, p_parts, mz, mz_parts, af, af_parts, total)
from sums
where not_numbers = 0 and (p <> p_parts or mz <> mz_parts or af <> af_parts or total <> p + mz + af)
