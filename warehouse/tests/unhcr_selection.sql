-- UNHCR default-aggregation guard (register GL-029): the API sums every dimension left out of a request into one
-- row ("-"). Each Silver entity must hold exactly its declared selection: population_by_asylum one row per
-- country of asylum with origins summed (coo '-', coa never '-'), poland_by_origin Poland as country of asylum
-- by named origins (coa POL, coo never '-'). Returns the offending rows.
select entity, year, coa, coo
from {{ ref('unhcr_population') }}
where
    (entity = 'population_by_asylum' and (coo <> '-' or coa = '-'))
    or (entity = 'poland_by_origin' and (coa_iso <> 'POL' or coo = '-'))
