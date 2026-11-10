{# Topics quality (docs/topics.md, Quality checks): the build fails on a gold.topic_quality row with status fail
   (a feed silent for 7 days, an active topic without signals). Warn rows: tests/topic_quality_warn.sql. #}
select
    check_key,
    subject,
    value,
    detail
from {{ ref('topic_quality') }}
where status = 'fail'
