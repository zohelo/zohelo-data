{{ config(alias='topic_quality') }}
{# Topics quality checks on the run day (docs/topics.md, Quality checks): one row per check × subject with a
   status (ok / warn / fail). feed_freshness: hours since the newest headline of each active outlet (warn and
   fail limits in warehouse.yaml topics.freshness_hours); matched_share: share of the last 7 days' headlines
   matched to any topic (warn outside topics.matched_share); topic_signals: an active topic without a GDELT
   query, a Wikipedia article or a Polish keyword (fail); volume_stale / attention_stale: an active topic whose
   newest GDELT or Wikipedia day is more than 3 days old (warn); unmatched_headlines: count of the last 7 days'
   headlines no topic matched (dictionary work). tests/topic_quality.sql fails the build on a fail row. #}
with as_of as (
    select {{ topics_as_of() }} as day
),

freshness as (
    select
        'feed_freshness' as check_key,
        o.outlet_key as subject,
        date_diff('hour', max(h.first_seen_at), now()) as value,
        case
            when
                max(h.first_seen_at) is null
                or date_diff('hour', max(h.first_seen_at), now()) > {{ topics_setting('freshness_hours.fail') }}
                then 'fail'
            when date_diff('hour', max(h.first_seen_at), now()) > {{ topics_setting('freshness_hours.warn') }}
                then 'warn'
            else 'ok'
        end as status,
        'hours since the newest headline' as detail
    from {{ ref('news_outlet') }} as o
    left join {{ ref('news_headlines') }} as h on o.outlet_key = h.outlet_key
    where o.status = 'active'
    group by o.outlet_key
),

recent as (
    select
        h.headline_id,
        exists(
            select 1 from {{ ref('topic_matches') }} as m
            where m.headline_id = h.headline_id
        ) as matched
    from {{ ref('news_headlines') }} as h
    cross join as_of as a
    where cast(timezone('Europe/Warsaw', h.published_at) as date) > a.day - 7
),

shares as (
    select
        'matched_share' as check_key,
        'all' as subject,
        avg(case when matched then 1.0 else 0.0 end) as value,
        case
            when
                avg(case when matched then 1.0 else 0.0 end) not between {{ topics_setting('matched_share.min') }}
                and {{ topics_setting('matched_share.max') }}
                then 'warn'
            else 'ok'
        end as status,
        count(*) || ' headlines in the last 7 days' as detail
    from recent
    union all
    select
        'unmatched_headlines' as check_key,
        'all' as subject,
        count(*) as value,
        'ok' as status,
        'headlines of the last 7 days no topic matched' as detail
    from recent
    where not matched
),

active as (
    select topic_key
    from {{ ref('topic') }}
    where status = 'active'
),

configured as (
    select
        'topic_signals' as check_key,
        a.topic_key as subject,
        0 as value,
        'fail' as status,
        'no ' || needed.what as detail
    from active as a
    cross join (values ('gdelt_query'), ('wiki_article')) as needed (what)
    where
        not exists (
            select 1 from {{ ref('topic_signal') }} as s
            where s.topic_key = a.topic_key and s.signal = needed.what
        )
    union all
    select
        'topic_signals' as check_key,
        a.topic_key as subject,
        0 as value,
        'fail' as status,
        'no Polish keyword' as detail
    from active as a
    where not exists (
        select 1 from {{ ref('topic_keyword') }} as k
        where k.topic_key = a.topic_key and k.lang = 'pl'
    )
),

stale as (
    select
        'volume_stale' as check_key,
        a.topic_key as subject,
        coalesce(date_diff('day', max(v.day), any_value(o.day)), -1) as value,
        case when max(v.day) is null or max(v.day) < any_value(o.day) - 3 then 'warn' else 'ok' end as status,
        'days since the newest GDELT day (-1: none)' as detail
    from active as a
    cross join as_of as o
    left join {{ ref('topic_volume') }} as v on a.topic_key = v.topic_key
    group by a.topic_key
    union all
    select
        'attention_stale' as check_key,
        a.topic_key as subject,
        coalesce(date_diff('day', max(w.day), any_value(o.day)), -1) as value,
        case when max(w.day) is null or max(w.day) < any_value(o.day) - 3 then 'warn' else 'ok' end as status,
        'days since the newest Wikipedia day (-1: none)' as detail
    from active as a
    cross join as_of as o
    left join {{ ref('topic_attention') }} as w on a.topic_key = w.topic_key
    group by a.topic_key
)

select
    check_key,
    subject,
    cast(value as double) as value,
    status,
    detail
from freshness
union all
select
    check_key,
    subject,
    cast(value as double) as value,
    status,
    detail
from shares
union all
select
    check_key,
    subject,
    cast(value as double) as value,
    status,
    detail
from configured
union all
select
    check_key,
    subject,
    cast(value as double) as value,
    status,
    detail
from stale
