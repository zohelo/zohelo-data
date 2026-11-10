{{ config(alias='topic_rank') }}
{# Topic ranking per period on the run day (docs/topics.md, Ranking): one row per period × ranked topic. Window =
   the period's last n days up to the run day (Warsaw); score = mean heat over the window's days with a heat (year:
   mean heat × share of those days with heat above year_hot_threshold). A topic enters the list only with at
   least min_headlines matched headlines in the window (day: min_headlines.day); ties are broken by headline
   count. trend compares the score with the window before (up / down beyond trend_flat, else flat). Active topics
   only. Settings: warehouse.yaml `topics:`. Source: gold.topic_daily. #}
{#- the published contract's periods (docs/published.md, topics.json); their lengths come from the settings -#}
{%- set periods = ['day', 'week', 'month', 'quarter', 'year'] -%}
with daily as (
    select
        d.topic_key,
        d.day,
        d.heat,
        d.headlines,
        d.z_news,
        d.z_volume,
        d.z_attention
    from {{ ref('topic_daily') }} as d
    inner join {{ ref('topic') }} as t on d.topic_key = t.topic_key
    where t.status = 'active'
),

windows as (
    {%- for period in periods %}
        select
            '{{ period }}' as period,
            {{ topics_setting('periods.' ~ period) }} as days,
            {{ topics_as_of() }} - {{ topics_setting('periods.' ~ period) }} + 1 as window_from,
            {{ topics_as_of() }} as window_to,
            case
                when '{{ period }}' = 'day' then {{ topics_setting('min_headlines.day') }}
                else {{ topics_setting('min_headlines.other') }}
            end as min_headlines
        {% if not loop.last %}union all{% endif %}
    {%- endfor %}
),

scored as (
    select
        w.period,
        w.window_from,
        w.window_to,
        w.min_headlines,
        d.topic_key,
        sum(case when d.day >= w.window_from then d.headlines else 0 end) as headlines,
        avg(case when d.day >= w.window_from then d.heat end) as mean_heat,
        avg(
            case
                when d.day >= w.window_from and d.heat is not null
                    then case when d.heat > {{ topics_setting('year_hot_threshold') }} then 1.0 else 0.0 end
            end
        ) as hot_share,
        avg(case when d.day < w.window_from then d.heat end) as prev_mean_heat,
        avg(
            case
                when d.day < w.window_from and d.heat is not null
                    then case when d.heat > {{ topics_setting('year_hot_threshold') }} then 1.0 else 0.0 end
            end
        ) as prev_hot_share,
        avg(case when d.day >= w.window_from then d.z_news end) as z_news,
        avg(case when d.day >= w.window_from then d.z_volume end) as z_volume,
        avg(case when d.day >= w.window_from then d.z_attention end) as z_attention
    from windows as w
    inner join daily as d on d.day between w.window_from - w.days and w.window_to
    group by w.period, w.window_from, w.window_to, w.min_headlines, d.topic_key
),

ranked as (
    select
        period,
        window_from,
        window_to,
        topic_key,
        headlines,
        case when period = 'year' then mean_heat * hot_share else mean_heat end as score,
        case when period = 'year' then prev_mean_heat * prev_hot_share else prev_mean_heat end as prev_score,
        z_news,
        z_volume,
        z_attention
    from scored
    where headlines >= min_headlines and mean_heat is not null
)

select
    period,
    window_from,
    window_to,
    row_number() over (partition by period order by score desc, headlines desc, topic_key asc) as rank,
    topic_key,
    score,
    headlines,
    case
        when prev_score is null or abs(score - prev_score) <= {{ topics_setting('trend_flat') }} then 'flat'
        when score > prev_score then 'up'
        else 'down'
    end as trend,
    prev_score,
    z_news,
    z_volume,
    z_attention
from ranked
