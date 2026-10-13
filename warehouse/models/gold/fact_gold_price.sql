{# NBP gold price: PLN per gram of 1000-fineness gold, one row per date. #}
select
    cast(strftime(effective_date, '%Y%m%d') as integer) as date_key,
    effective_date as price_date,
    'PLN' as currency_code,
    'nbp' as source_system,
    price_pln_per_gram,
    raw_sha256
from {{ ref('nbp_gold_fixings') }}
