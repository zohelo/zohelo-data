{# Public attention per topic and day (topics, docs/topics.md): Wikipedia user views summed over the topic's
   articles (wiki_article rows of seed topic_signal), from bronze.wiki_pageviews (every landed copy; the newest
   value per article and day wins). Grain: topic_key × day. #}
with views as (
    select
        entity,
        project,
        article,
        strptime(day, '%Y%m%d')::date as day,
        views,
        raw_sha256
    from {{ source('wikimedia_pageviews', 'wiki_pageviews') }}
    qualify row_number() over (partition by entity, day order by ingested_at desc, raw_sha256 asc) = 1
),

articles as (
    select
        topic_key,
        split_part(value, ':', 1) || '.wikipedia' as project,
        replace(substr(value, strpos(value, ':') + 1), ' ', '_') as article
    from {{ ref('topic_signal') }}
    where signal = 'wiki_article'
)

select
    a.topic_key,
    v.day,
    sum(v.views) as views,
    count(*) as articles,
    string_agg(v.project || ':' || v.article, ';' order by v.project, v.article) as article_list,
    min(v.raw_sha256) as raw_sha256
from views as v
inner join articles as a on v.project = a.project and v.article = a.article
group by a.topic_key, v.day
