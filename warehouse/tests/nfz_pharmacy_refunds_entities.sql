-- NFZ pharmacy refunds: every declared entity (macro poc_nfz_pharmacy_refunds_entities, the list the extractor
-- lands) has Silver rows and every Silver entity is declared; every Bronze entity yields Silver rows; each entity
-- holds exactly the months the API announced (meta.count of its newest download). Returns the offending entities.
with declared as (select entity from ({{ poc_nfz_pharmacy_refunds_entities() }})),

silver as (
    select
        entity,
        count(*) as months,
        min(meta_count) as announced_min,
        max(meta_count) as announced_max
    from {{ ref('nfz_ra_monthly_provisions') }}
    group by entity
),

bronze as (
    select distinct entity from {{ source('nfz_pharmacy_refunds', 'nfz_ra_monthly_provisions') }}
)

select
    coalesce(d.entity, s.entity) as entity,
    case
        when s.entity is null then 'declared, no Silver rows'
        when d.entity is null then 'in Silver, not declared'
        else s.months || ' months, meta.count ' || s.announced_min || '-' || s.announced_max
    end as problem
from declared as d
full outer join silver as s on d.entity = s.entity
where d.entity is null or s.entity is null or s.months <> s.announced_min or s.announced_min <> s.announced_max
union all
select
    b.entity,
    'in Bronze, no Silver rows' as problem
from bronze as b
left join silver as s on b.entity = s.entity
where s.entity is null
