-- ZUS statistical portal: every voivodeship table period has Poland and all 16 voivodeships for each measure and
-- breakdown. A voivodeship name the model does not know would drop that area silently from insured_sickness_voiv
-- (whose powiat rows are left out by design); returns the family × period × measure × breakdown with another count.
select
    family,
    period_start,
    measure,
    breakdown_code,
    count(distinct area_code) as areas
from {{ ref('zus_psz_observations') }}
where family in ('insured_sickness_voiv', 'payers_voiv', 'absence_voiv') and area_code <> '_U'
group by all
having count(distinct area_code) <> 17
