{{ config(alias='parliament_sejm_votings') }}
{# Gold entity table (events): one row per voting of the Sejm, terms 9 and 10 (register PLG-024), keyed by the
   API's term, sitting number and voting number; date_key = the day of the voting. Title, topic and totals as
   published (empty yes, no and abstain for list and traditional votes; a traditional vote's totals sent as 0, not
   counted, are empty too, and so is a present or total_voted of 0 of any kind). Individual MPs' votes are not kept. #}
select
    term,
    sitting_number,
    voting_number,
    voting_date as date_key,
    voted_at,
    sitting_day,
    kind,
    is_list_vote,
    majority_type,
    majority_votes,
    title,
    topic,
    description,
    yes,
    no,
    abstain,
    not_participating,
    total_voted,
    present,
    against_all,
    'sejm' as source_system,
    source_version,
    raw_sha256
from {{ ref('sejm_votings') }}
