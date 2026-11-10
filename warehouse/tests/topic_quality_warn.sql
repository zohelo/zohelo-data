{# Topics quality warnings (docs/topics.md, Quality checks): a feed silent for 48 hours, matched share outside its
   range, stale GDELT or Wikipedia signals. The build warns, it does not fail. #}
{{ config(severity='warn') }}
select
    check_key,
    subject,
    value,
    detail
from {{ ref('topic_quality') }}
where status = 'warn'
