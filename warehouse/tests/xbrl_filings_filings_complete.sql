{# silver.xbrl_filings_filings is complete: every filing of the newest Bronze copy of the filing list
   (bronze.xbrl_filings_filings) is in Silver. One row per filing missing from Silver (fails). #}
with newest as (
    select fxo_id
    from {{ source('xbrl_filings', 'xbrl_filings_filings') }}
    qualify ingested_at = max(ingested_at) over (partition by entity)
)

select n.fxo_id
from newest as n
left join {{ ref('xbrl_filings_filings') }} as s on n.fxo_id = s.fxo_id
where s.fxo_id is null
