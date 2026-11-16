{# Sejm API (register PLG-024): votings of terms 9 and 10.
   Grain: term x sitting_number x voting_number (the API's own numbering, per sitting). Source: bronze.sejm_votings
   (one answer per sitting), with the number of votings the API lists for the sitting in bronze.sejm_voting_days
   (votings_listed, for the completeness test). Totals as published. A list vote (kind ON_LIST, e.g. electing the
   Marshal) carries its result per option, which is not loaded; the API sends 0 for its yes, no and abstain, so
   they are empty here (is_list_vote), never 0. Individual MPs' votes are not read. #}
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
    case when v.kind <> 'ON_LIST' then v.yes end as yes,
    case when v.kind <> 'ON_LIST' then v.no end as no,
    case when v.kind <> 'ON_LIST' then v.abstain end as abstain,
    v.not_participating,
    v.total_voted,
    v.present,
    v.against_all,
    v.voting_options,
    l.votings_listed,
    v.source_version,
    v.raw_sha256
from votings as v
left join listed as l on v.term = l.term and v.sitting_number = l.sitting_number
