{# Topic seeds (docs/topics.md): every keyword pattern and exclusion compiles as RE2 (an invalid one errors);
   weights lie in (0, 1]; one row per topic and KPI; every active topic has a GDELT query and a Wikipedia
   article (fail), and at least one keyword in Polish. Returns the offending rows. #}
with keywords as (
    select
        topic_key,
        'weight out of (0, 1]: ' || pattern as problem
    from {{ ref('topic_keyword') }}
    where
        weight <= 0 or weight > 1
        or regexp_matches('x', '(^|[^\pL\pN])(' || pattern || ')') is null
        or regexp_matches('x', '(^|[^\pL\pN])(' || coalesce(exclude, 'x') || ')') is null
),

kpis as (
    select
        topic_key,
        'duplicate KPI ' || kpi_key as problem
    from {{ ref('topic_kpi') }}
    group by topic_key, kpi_key
    having count(*) > 1
),

coverage as (
    select
        t.topic_key,
        'active topic without ' || needed.what as problem
    from {{ ref('topic') }} as t
    cross join (values ('gdelt_query'), ('wiki_article'), ('pl')) as needed (what)
    where
        t.status = 'active'
        and not exists (
            select 1 from {{ ref('topic_signal') }} as s
            where s.topic_key = t.topic_key and s.signal = needed.what
        )
        and not exists (
            select 1 from {{ ref('topic_keyword') }} as k
            where k.topic_key = t.topic_key and k.lang = needed.what
        )
)

select * from keywords
union all
select * from kpis
union all
select * from coverage
