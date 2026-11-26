-- wipo_ip_stats: every entity in Bronze yields Silver rows (patent_indicators in both patent models,
-- design_indicators in the design model). A ZIP whose layout the Silver models no longer read would otherwise drop
-- out silently; returns the entity and the model without rows.
with bronze_entities as (
    select distinct entity
    from {{ source('wipo_ip_stats', 'wipo_ip_stats_counts') }}
),

silver as (
    select distinct
        'wipo_ip_stats_patent_publications' as model,
        entity
    from {{ ref('wipo_ip_stats_patent_publications') }}
    union all
    select distinct
        'wipo_ip_stats_patent_grants',
        entity
    from {{ ref('wipo_ip_stats_patent_grants') }}
    union all
    select distinct
        'wipo_ip_stats_designs',
        entity
    from {{ ref('wipo_ip_stats_designs') }}
),

expected as (
    select * from (
        values
        ('patent_indicators', 'wipo_ip_stats_patent_publications'),
        ('patent_indicators', 'wipo_ip_stats_patent_grants'),
        ('design_indicators', 'wipo_ip_stats_designs')
    ) as t (entity, model)
)

select
    b.entity,
    e.model
from bronze_entities as b
left join expected as e on b.entity = e.entity
left join silver as s on e.entity = s.entity and e.model = s.model
where s.entity is null
