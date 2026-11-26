{# silver.xbrl_filings_filings: the issuer's LEI (the entity the API attaches to the filing) must be the first 20
   characters of the filing id, and 20 characters of A-Z and 0-9 (ISO 17442 shape). One row per filing that
   breaks this (fails). #}
select
    fxo_id,
    lei
from {{ ref('xbrl_filings_filings') }}
where lei <> substr(fxo_id, 1, 20) or not regexp_full_match(lei, '[A-Z0-9]{18}[0-9]{2}')
