-- GUGiK PRG build stage, block punkty_adresowe (#3106): every count of address points is a whole number between 1
-- and 10,000,000 (Poland, the largest area: 8,647,096 points in the package of 26 Nov 2026, Dev trial 45827301180). Above
-- the bound means points counted twice; a zero or a fraction means a value that is not a count. A live test on the
-- build stage, as the source has no metrics model of this measure. Returns the rows outside.
select
    measure_key,
    geography_key,
    date_key,
    value
from {{ ref('poc_build_prg') }}
where block_id = 'punkty_adresowe' and (value < 1 or value > 1e7 or value <> floor(value))
