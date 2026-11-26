{# Sejm API (register PLG-024): votings of terms 9 and 10.
   Grain: term x sitting_number x voting_number (the API's own numbering, per sitting). Source: bronze.sejm_votings
   (one answer per sitting), with the number of votings the API lists for the sitting in bronze.sejm_voting_days
   (votings_listed, for the completeness test). Totals as published for electronic votes. A list vote (kind
   ON_LIST, e.g. electing the Marshal) carries its result per option, which is not loaded, and a traditional vote
   (TRADITIONAL) has no counted totals: the API sends 0 for their yes, no and abstain (Dev trial: all 31 such votes),
   so they are empty here, never 0. A traditional vote's total_voted, present, not_participating and against_all are
   0 as well when nothing was counted (Dev trial 45773844753: all four 0 on all 16), so a 0 there is empty too. A
   list vote keeps its real totals (419 to 459 voted) but its present is 0 on all 15 (not counted): present and
   total_voted of 0 are empty for every kind (a held vote has someone present). Individual MPs' votes are not
   read. #}
with votings as (
    select
        cast(term as integer) as term,
        cast(item_term as integer) as item_term,
        cast(sitting_number as integer) as sitting_number,
        cast(sitting_day as integer) as sitting_day,
        cast(voting_number as integer) as voting_number,
        cast(voted_at as timestamp) as voted_at,
        kind,
        majority_type,
        cast(majority_votes as integer) as majority_votes,
        title,
        topic,
        description,
        cast(yes as integer) as yes,
        cast(no as integer) as no,
        cast(abstain as integer) as abstain,
        cast(not_participating as integer) as not_participating,
        cast(total_voted as integer) as total_voted,
        cast(present as integer) as present,
        cast(against_all as integer) as against_all,
        cast(voting_options as integer) as voting_options,
        source_version,
        raw_sha256
    from {{ source('sejm', 'sejm_votings') }}
),

listed as (
    select
        cast(term as integer) as term,
        cast(sitting_number as integer) as sitting_number,
        sum(cast(votings_num as integer)) as votings_listed
    from {{ source('sejm', 'sejm_voting_days') }}
    group by all
)

select
    v.term,
    v.item_term,
    v.sitting_number,
    v.sitting_day,
    v.voting_number,
    v.voted_at,
    cast(v.voted_at as date) as voting_date,
    v.kind,
    v.kind = 'ON_LIST' as is_list_vote,
    v.majority_type,
    v.majority_votes,
    v.title,
    v.topic,
    v.description,
    case when v.kind = 'ELECTRONIC' then v.yes end as yes,
    case when v.kind = 'ELECTRONIC' then v.no end as no,
    case when v.kind = 'ELECTRONIC' then v.abstain end as abstain,
    case when v.kind = 'TRADITIONAL' then nullif(v.not_participating, 0) else v.not_participating end
        as not_participating,
    nullif(v.total_voted, 0) as total_voted,
    nullif(v.present, 0) as present,
    case when v.kind = 'TRADITIONAL' then nullif(v.against_all, 0) else v.against_all end as against_all,
    v.voting_options,
    l.votings_listed,
    v.source_version,
    v.raw_sha256
from votings as v
left join listed as l on v.term = l.term and v.sitting_number = l.sitting_number
