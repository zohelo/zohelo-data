-- NFZ pharmacy refunds: the 16 voivodeships partition Poland. For every ATC selection and month, the sum over the
-- provinces of each published value equals the unfiltered national value (Dev probe 43310762847: exact for
-- 2024-12). The API answers an unknown province code with HTTP 200 instead of an error, so a province filter it
-- stopped applying would show here as 16 copies of Poland. A check only, nothing is stored. Returns the months off.
with provinces as (
    select
        atc_code,
        period_start,
        count(*) as areas,
        sum(quantity) as quantity,
        sum(refund) as refund,
        sum(donation) as donation,
        sum(contribution_of_patient) as contribution_of_patient,
        sum(value) as value
    from {{ ref('nfz_ra_monthly_provisions') }}
    where area_code <> 'PL'
    group by atc_code, period_start
)

select
    p.atc_code,
    p.period_start,
    p.areas,
    p.refund - n.refund as refund_difference,
    p.value - n.value as value_difference
from provinces as p
inner join {{ ref('nfz_ra_monthly_provisions') }} as n
    on n.area_code = 'PL' and p.atc_code = n.atc_code and p.period_start = n.period_start
where
    abs(p.quantity - n.quantity) > 0.01
    or abs(p.refund - n.refund) > 0.05
    or abs(p.donation - n.donation) > 0.05
    or abs(p.contribution_of_patient - n.contribution_of_patient) > 0.05
    or abs(p.value - n.value) > 0.05
