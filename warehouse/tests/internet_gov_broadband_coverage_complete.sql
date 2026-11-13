-- internet_gov_broadband (register PL-ENV-034): every quarter between the first and the last edition is present and
-- complete. Per reference date: the territory table holds exactly the 16 voivodeships of
-- poc_internet_gov_broadband_voivodeships() (every printed name mapped to a TERC code, nothing else) plus Poland; the
-- bandwidth classes and the service types (actual + wholesale only + theoretical only = in coverage) add up to their SUMA row (address points and coverages), and the
-- Poland totals of the three tables agree. Bronze applies the same sums to each edition; this test keeps them true
-- for the model as built. Refs only this source's model.
with s as (
    select * from {{ ref('internet_gov_broadband_coverage') }}
),

voivodeships as (
    {{ poc_internet_gov_broadband_voivodeships() }}
),

dates as (
    select distinct reference_date from s
    union
    select cast(last_day(q + interval 2 month) as date)
    from (
        select unnest(generate_series(
            date_trunc('quarter', min(reference_date)), date_trunc('quarter', max(reference_date)), interval 3 month
        )) as q
        from s
    )
),

territory as (
    select
        reference_date,
        count(*) filter (where area_code in (select area_code from voivodeships)) as voivodeships,
        count(*) filter (where area_code = 'PL') as poland,
        count(*) filter (where area_code is null or (area_code <> 'PL' and area_code not in (select area_code from voivodeships)))
            as other,
        sum(address_points) filter (where area_code <> 'PL') as voiv_points,
        sum(address_points_total) filter (where area_code <> 'PL') as voiv_total,
        max(address_points) filter (where area_code = 'PL') as pl_points,
        max(address_points_total) filter (where area_code = 'PL') as pl_total
    from s
    where table_code = 'territory'
    group by reference_date
),

classes as (
    select
        reference_date,
        table_code,
        sum(address_points) filter (where class_code <> '_T') as class_points,
        max(address_points) filter (where class_code = '_T') as sum_points,
        coalesce(sum(coverages) filter (where class_code <> '_T'), 0) as class_coverages,
        coalesce(max(coverages) filter (where class_code = '_T'), 0) as sum_coverages,
        sum(address_points) filter (where class_code not in ('_T', 'brak_zasiegu')) as covered_points
    from s
    where table_code in ('bandwidth', 'service_type')
    group by reference_date, table_code
),

checks as (
    select
        d.reference_date,
        coalesce(t.voivodeships, 0) = 16 and coalesce(t.poland, 0) = 1 and coalesce(t.other, 0) = 0 as territory_ok,
        t.voiv_points = t.pl_points and t.voiv_total = t.pl_total as territory_sums_ok,
        b.class_points = b.sum_points and b.class_coverages = b.sum_coverages and b.sum_points = t.pl_points
            as bandwidth_ok,
        r.class_points = r.sum_points and r.sum_points = t.pl_total and r.covered_points = t.pl_points
            as service_type_ok
    from dates as d
    left join territory as t on d.reference_date = t.reference_date
    left join classes as b on d.reference_date = b.reference_date and b.table_code = 'bandwidth'
    left join classes as r on d.reference_date = r.reference_date and r.table_code = 'service_type'
)

select *
from checks
where not coalesce(territory_ok and territory_sums_ok and bandwidth_ok and service_type_ok, false)
