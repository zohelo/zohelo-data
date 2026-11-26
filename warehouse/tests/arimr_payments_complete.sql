-- ARiMR direct-payment statistics (silver.arimr_payments_direct_payments): every campaign from 2004 (the first year
-- of direct payments in Poland, printed in the reports) to the newest one of each measure has all 17 areas (Poland =
-- "Razem" and the 16 voivodeships). A table, title or header that stops being read in a new report leaves a gap and
-- fails here. Returns the measure x campaign cells with another number of areas.
with counts as (
    select
        measure_code,
        campaign_year,
        count(distinct area_name) as n
    from {{ ref('arimr_payments_direct_payments') }}
    group by all
),

grid as (
    select
        m.measure_code,
        unnest(generate_series(2004, m.last_year)) as campaign_year
    from (
        select
            measure_code,
            max(campaign_year) as last_year
        from counts
        group by all
    ) as m
),

expected as (
    select
        g.measure_code,
        g.campaign_year
    from grid as g
    union
    select
        m.measure_code,
        null as campaign_year
    from (values ('applications'), ('decisions'), ('amount_paid')) as m (measure_code)
    where m.measure_code not in (select c.measure_code from counts as c)
)

select
    e.measure_code,
    e.campaign_year,
    coalesce(c.n, 0) as found,
    17 as expected
from expected as e
left join counts as c on e.measure_code = c.measure_code and e.campaign_year = c.campaign_year
where coalesce(c.n, 0) <> 17
