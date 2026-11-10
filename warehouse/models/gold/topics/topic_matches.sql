{{ config(alias='topic_matches') }}
{# Headlines matched to topics (docs/topics.md, Matching): one row per headline × topic. A headline matches when
   one of the topic's patterns for its language (seed topic_keyword, RE2 on the lower-case title, anchored at a
   word start) occurs and that row's exclusion does not. weight = the strongest matching row's weight;
   pattern = that row (traceable to a dictionary line); patterns = every row that hit. day = publication day in
   Warsaw. Grain: headline_id × topic_key. Source: silver.news_headlines. #}
with headlines as (
    select
        headline_id,
        outlet_key,
        lang,
        title,
        url,
        published_at,
        lower(title) as title_lower
    from {{ ref('news_headlines') }}
),

keywords as (
    select
        k.topic_key,
        k.lang,
        k.pattern,
        k.weight,
        k.exclude
    from {{ ref('topic_keyword') }} as k
    inner join {{ ref('topic') }} as t on k.topic_key = t.topic_key
),

hits as (
    select
        h.headline_id,
        h.outlet_key,
        h.lang,
        h.title,
        h.url,
        h.published_at,
        k.topic_key,
        k.pattern,
        k.weight
    from headlines as h
    inner join keywords as k
        on
            h.lang = k.lang
            and regexp_matches(h.title_lower, '(^|[^\pL\pN])(' || k.pattern || ')')
            and (k.exclude is null or not regexp_matches(h.title_lower, '(^|[^\pL\pN])(' || k.exclude || ')'))
)

select
    headline_id,
    topic_key,
    any_value(outlet_key) as outlet_key,
    any_value(lang) as lang,
    any_value(title) as title,
    any_value(url) as url,
    any_value(published_at) as published_at,
    cast(timezone('Europe/Warsaw', any_value(published_at)) as date) as day,
    max(weight) as weight,
    arg_max(pattern, weight) as pattern,
    string_agg(pattern, ' ; ' order by weight desc, pattern asc) as patterns
from hits
group by headline_id, topic_key
