-- GUGiK BDOT10k (silver.bdot10k_powiat_packages): the package index is complete. The index of 26 Nov 2026 held all
-- 380 powiats (314 land powiats and 66 cities with powiat rights) in all 16 voivodeships; fewer than 375 packages or
-- fewer than 16 voivodeships (a truncated or wrong answer that still passed the Bronze layout checks) fails here.
select
    count(*) as packages,
    count(distinct voivodeship_code) as voivodeships
from {{ ref('bdot10k_powiat_packages') }}
having count(*) < 375 or count(distinct voivodeship_code) < 16
