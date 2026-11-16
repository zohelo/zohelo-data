{# silver.xbrl_filings_facts is complete: for every filing of the current list, the entity-level facts of its
   newest Bronze facts document (no dimension beyond concept, entity, period and unit; a duration period; a unit;
   a numeric value) are all counted in Silver (sum of facts). A fact without a number (nil) is no fact here, never
   a zero. One row per filing whose counts differ (fails). #}
with newest as (
    select *
    from {{ source('xbrl_filings', 'xbrl_filings_facts') }}
    qualify ingested_at = max(ingested_at) over (partition by entity)
),

bronze as (
    select
        fxo_id,
        count(*) as facts
    from newest
    where
        extra_dimensions = '{}'
        and contains(period, '/')
        and unit is not null
        and try_cast(value as double) is not null
    group by fxo_id
),

silver as (
    select
        fxo_id,
        sum(facts) as facts
    from {{ ref('xbrl_filings_facts') }}
    group by fxo_id
)

select
    b.fxo_id,
    b.facts as bronze_facts,
    coalesce(s.facts, 0) as silver_facts
from bronze as b
inner join {{ ref('xbrl_filings_filings') }} as f on b.fxo_id = f.fxo_id
left join silver as s on b.fxo_id = s.fxo_id
where coalesce(s.facts, 0) <> b.facts
