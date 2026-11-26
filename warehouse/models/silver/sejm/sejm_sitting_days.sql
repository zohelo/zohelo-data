{# Sejm API (register PLG-024): sitting days of terms 9 and 10.
   Grain: term x sitting_number x day_date. Source: bronze.sejm_proceedings (each numbered sitting with its dates;
   entries numbered 0 are planned sittings without a number and are left out, the Dev trial counts them) and
   bronze.sejm_voting_days (the number of votings the API lists for the day; empty on a day without votes, never 0).
   is_held: the day is not after the build date (a sitting's later days are planned; a landed copy is not compared,
   because an unchanged answer lands no new copy). Per sitting, for the completeness test: sitting_votings_listed and
   sitting_voting_days_listed (votings and days with votes in the voting days list) and sitting_votings_loaded (votings
   rows of the sitting in bronze.sejm_votings). #}
with proceedings as (
    select
        cast(term as integer) as term,
        cast(sitting_number as integer) as sitting_number,
        title as sitting_title,
        is_current = 'true' as is_current_sitting,
        dates,
        source_version,
        raw_sha256
    from {{ source('sejm', 'sejm_proceedings') }}
    where cast(sitting_number as integer) > 0
),

days as (
    select
        p.*,
        cast(unnest(string_split(p.dates, '|')) as date) as day_date
    from proceedings as p
    where coalesce(p.dates, '') <> ''
),

voting_days as (
    select
        cast(term as integer) as term,
        cast(sitting_number as integer) as sitting_number,
        cast(voting_date as date) as day_date,
        cast(votings_num as integer) as votings_listed
    from {{ source('sejm', 'sejm_voting_days') }}
),

listed as (
    select
        term,
        sitting_number,
        sum(votings_listed) as sitting_votings_listed,
        count(*) as sitting_voting_days_listed
    from voting_days
    group by all
),

loaded as (
    select
        cast(term as integer) as term,
        cast(sitting_number as integer) as sitting_number,
        count(*) as sitting_votings_loaded
    from {{ source('sejm', 'sejm_votings') }}
    group by all
)

select
    d.term,
    d.sitting_number,
    d.day_date,
    d.sitting_title,
    d.is_current_sitting,
    d.day_date <= current_date as is_held,
    v.votings_listed,
    coalesce(l.sitting_votings_listed, 0) as sitting_votings_listed,
    coalesce(l.sitting_voting_days_listed, 0) as sitting_voting_days_listed,
    coalesce(o.sitting_votings_loaded, 0) as sitting_votings_loaded,
    d.source_version,
    d.raw_sha256
from days as d
left join voting_days as v
    on d.term = v.term and d.sitting_number = v.sitting_number and d.day_date = v.day_date
left join listed as l on d.term = l.term and d.sitting_number = l.sitting_number
left join loaded as o on d.term = o.term and d.sitting_number = o.sitting_number
