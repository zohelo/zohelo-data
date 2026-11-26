-- GUGiK PRNG (silver.prng_object_gminas): the localities cover the gminas of Poland (2,477 gminas; the package of
-- 12 Nov 2026 named 2,479 distinct gmina codes for its localities) and every voivodeship. Fewer than 2,400 gmina codes
-- or 16 voivodeship codes means the territory columns were not read.
select
    count(distinct gmina_code) as gmina_codes,
    count(distinct voivodeship_code) as voivodeship_codes
from {{ ref('prng_object_gminas') }}
where entity = 'miejscowosci'
having count(distinct gmina_code) < 2400 or count(distinct voivodeship_code) < 16
