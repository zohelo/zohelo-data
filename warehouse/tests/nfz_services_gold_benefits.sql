-- NFZ services: (1) every benefit with a published Polish whole-year number of hospitalisations in
-- silver.nfz_services_basic_data has that series in gold.build_nfz_services, and (2) every JGP group of
-- poc_nfz_services_detail() (the KPI groups) is loaded in Silver, so a product code that NFZ retires or a typo in
-- the list is seen, and (3) every JGP group a kpi_measure.csv row of nfz_services reads is in
-- poc_nfz_services_detail() (Landing's canaries are derived from those rows, so the two lists cannot drift).
-- Returns the benefit and the problem.
with silver as (
    select catalog, benefit_code, count(*) as years
    from {{ ref('nfz_services_basic_data') }}
    where period_kind = 'year' and area_code = 'PL' and area_records = 1 and number_of_hospitalizations is not null
    group by all
),

gold as (
    select
        block_id as catalog,
        split_part(measure_codes, '|', 1) as benefit_code,
        count(*) as years
    from {{ ref('poc_build_nfz_services') }}
    where geography_key = 'PL' and measure_codes like '%|number-of-hospitalizations'
    group by all
),

detail as ({{ poc_nfz_services_detail() }}),

kpi as (
    select distinct
        split_part(measure_key_pattern, ':', 2) as catalog,
        split_part(measure_key_pattern, ':', 4) as benefit_code
    from {{ ref('kpi_measure') }}
    where source_system = 'nfz_services'
)

select
    s.catalog,
    s.benefit_code,
    'Polish hospitalisations in Silver for ' || s.years || ' years, in Gold for ' || coalesce(g.years, 0) as problem
from silver as s
left join gold as g on s.catalog = g.catalog and s.benefit_code = g.benefit_code
where coalesce(g.years, 0) <> s.years
union all
select
    d.catalog,
    d.benefit_code,
    'detail group not loaded in Silver' as problem
from detail as d
left join silver as s on d.catalog = s.catalog and d.benefit_code = s.benefit_code
where s.benefit_code is null
union all
select
    k.catalog,
    k.benefit_code,
    'KPI group (kpi_measure.csv) not in poc_nfz_services_detail()' as problem
from kpi as k
left join detail as d on k.catalog = d.catalog and k.benefit_code = d.benefit_code
where d.benefit_code is null
