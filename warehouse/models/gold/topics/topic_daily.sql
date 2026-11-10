{{ config(alias='topic_daily') }}
{# Topic heat per day (docs/topics.md, Heat score): one row per topic × day for the last history_days up to the
   run day (Warsaw). Signals: news = matched headlines weighted by pattern weight (0 on days the feeds ran with no
   match), volume = GDELT matching articles per volume_per monitored, attention = Wikipedia views. Each signal's
   robust z-score uses the topic's own trailing baseline_days (median, MAD with mad_floor; no z-score with fewer
   than min_baseline_days observed). heat = weighted mean of the available z-scores (missing signals drop out and
   the weights are rescaled) + release (release.day on the release day of a main KPI, release.day_before the day
   before). Settings: warehouse.yaml `topics:`. Sources: gold.topic_matches, silver.topic_volume,
   silver.topic_attention, silver.releases (official calendars), seeds release_calendar (manual fallback),
   kpi_v2_map, topic_kpi. #}
{%- set w = topics_setting('weights') or {} -%}
{%- set b = topics_setting('baseline_days') -%}
with days as (
    select cast(d as date) as day
    from generate_series(
        {{ topics_as_of() }} - {{ topics_setting('history_days') }}, {{ topics_as_of() }}, interval 1 day
    ) as g (d)
),

grid as (
    select
        t.topic_key,
        d.day
    from {{ ref('topic') }} as t
    cross join days as d
),

news_start as (
    select min(cast(timezone('Europe/Warsaw', first_seen_at) as date)) as day
    from {{ ref('news_headlines') }}
),

news as (
    select
        topic_key,
        day,
        count(*) as headlines,
        sum(weight) as news
    from {{ ref('topic_matches') }}
    group by topic_key, day
),

release_kpis as (
    select distinct
        m.v2_key as kpi_key,
        cast(timezone('Europe/Warsaw', cast(r.release_at as timestamptz)) as date) as day
    from {{ ref('release_calendar') }} as r
    inner join {{ ref('kpi_v2_map') }} as m on r.kpi_key = m.concept_key
    union
    select
        trim(k.kpi_key) as kpi_key,
        r.release_date as day
    from {{ ref('releases') }} as r
    cross join unnest(string_split(r.kpi_keys, ';')) as k (kpi_key)
    where r.kpi_keys != ''
),

release_days as (
    select distinct
        k.topic_key,
        r.day
    from release_kpis as r
    inner join {{ ref('topic_kpi') }} as k on r.kpi_key = k.kpi_key and k.role = 'main'
),

signals as (
    select
        g.topic_key,
        g.day,
        coalesce(n.headlines, 0) as headlines,
        case when g.day >= s.day then coalesce(n.news, 0) end as news,
        v.volume_share * {{ topics_setting('volume_per') }} as volume,
        a.views as attention,
        v.tone,
        case
            when
                exists (
                    select 1 from release_days as r
                    where r.topic_key = g.topic_key and r.day = g.day
                )
                then {{ topics_setting('release.day') }}
            when
                exists (
                    select 1 from release_days as r
                    where r.topic_key = g.topic_key and r.day = g.day + 1
                )
                then {{ topics_setting('release.day_before') }}
            else 0
        end as release
    from grid as g
    cross join news_start as s
    left join news as n on g.topic_key = n.topic_key and g.day = n.day
    left join {{ ref('topic_volume') }} as v on g.topic_key = v.topic_key and g.day = v.day
    left join {{ ref('topic_attention') }} as a on g.topic_key = a.topic_key and g.day = a.day
),

baselines as (
    select
        *,
        median(news) over baseline_window as news_median,
        mad(news) over baseline_window as news_mad,
        count(news) over baseline_window as news_n,
        median(volume) over baseline_window as volume_median,
        mad(volume) over baseline_window as volume_mad,
        count(volume) over baseline_window as volume_n,
        median(attention) over baseline_window as attention_median,
        mad(attention) over baseline_window as attention_mad,
        count(attention) over baseline_window as attention_n
    from signals
    window baseline_window as (partition by topic_key order by day rows between {{ b }} preceding and 1 preceding)
),

scores as (
    select
        *,
        case
            when news_n >= {{ topics_setting('min_baseline_days') }}
                then (news - news_median) / greatest(news_mad, {{ topics_setting('mad_floor') }})
        end as z_news,
        case
            when volume_n >= {{ topics_setting('min_baseline_days') }}
                then (volume - volume_median) / greatest(volume_mad, {{ topics_setting('mad_floor') }})
        end as z_volume,
        case
            when attention_n >= {{ topics_setting('min_baseline_days') }}
                then (attention - attention_median) / greatest(attention_mad, {{ topics_setting('mad_floor') }})
        end as z_attention
    from baselines
)

select
    topic_key,
    day,
    headlines,
    news,
    volume,
    attention,
    tone,
    z_news,
    z_volume,
    z_attention,
    release,
    (
        coalesce({{ w.news }} * z_news, 0) + coalesce({{ w.volume }} * z_volume, 0)
        + coalesce({{ w.attention }} * z_attention, 0)
    )
    / nullif(
        case when z_news is null then 0 else {{ w.news }} end
        + case when z_volume is null then 0 else {{ w.volume }} end
        + case when z_attention is null then 0 else {{ w.attention }} end,
        0
    )
    + release as heat
from scores
