{# News volume and tone per topic and day (topics, docs/topics.md) from bronze.gdelt_timeline (GDELT DOC 2.0
   API, every landed copy; the newest value per topic, mode and day wins). volume = matching articles,
   monitored = all articles GDELT monitored that day, volume_share = volume / monitored (the normalised volume
   used by the heat score), tone = average tone (negative = negative coverage). Grain: topic_key × day. #}
with points as (
    select
        topic_key,
        mode,
        strptime(left(date_text, 8), '%Y%m%d')::date as day,
        try_cast(value_text as double) as value,
        try_cast(norm_text as double) as norm,
        raw_sha256
    from {{ source('gdelt', 'gdelt_timeline') }}
    qualify row_number() over (
        partition by topic_key, mode, left(date_text, 8) order by ingested_at desc, raw_sha256 asc
    ) = 1
)

select
    topic_key,
    day,
    max(case when mode = 'volraw' then value end) as volume,
    max(case when mode = 'volraw' then norm end) as monitored,
    max(case when mode = 'volraw' then value end) / nullif(max(case when mode = 'volraw' then norm end), 0)
        as volume_share,
    max(case when mode = 'tone' then value end) as tone,
    min(raw_sha256) as raw_sha256
from points
group by topic_key, day
