{# Senate of the Republic of Poland (register PLG-025, route D36): votes of term XI.
   Grain: vote_id (the Senate's id in glosowanie-drukuj,<vote_id>.html); vote_number is the printed number, counted
   per sitting across its days. Source: bronze.senat_day_votes (the votes listed on each sitting day: number,
   subject, subtitle) with the day's date from silver.senat_sitting_days, and bronze.senat_vote_results (the totals
   printed on the vote's page: głosowało = voted, za, przeciw, wstrzymało się). results_landed is false while the
   vote's page is not read yet (the first load takes several runs): its totals are then empty, never 0. Totals are
   as printed; individual senators' votes are not kept. #}
with listed as (
    select
        cast(vote_id as bigint) as vote_id,
        sitting_id,
        cast(day_number as integer) as day_number,
        cast(vote_number as integer) as vote_number,
        subject,
        subtitle,
        source_version,
        raw_sha256
    from {{ source('senat', 'senat_day_votes') }}
    where vote_id is not null
),

days as (
    select
        sitting_id,
        day_number,
        sitting_number,
        day_date,
        is_finished
    from {{ ref('senat_sitting_days') }}
),

results as (
    select
        cast(vote_id as bigint) as vote_id,
        title as result_title,
        try_cast(voted as integer) as voted,
        try_cast(votes_for as integer) as votes_for,
        try_cast(votes_against as integer) as votes_against,
        try_cast(abstained as integer) as abstained,
        try_cast(senators_listed as integer) as senators_listed,
        mark_counts,
        source_version as result_version,
        raw_sha256 as result_raw_sha256
    from {{ source('senat', 'senat_vote_results') }}
)

select
    l.vote_id,
    l.sitting_id,
    d.sitting_number,
    l.day_number,
    d.day_date,
    l.vote_number,
    l.subject,
    l.subtitle,
    d.is_finished,
    r.vote_id is not null as results_landed,
    r.voted,
    r.votes_for,
    r.votes_against,
    r.abstained,
    r.senators_listed,
    r.mark_counts,
    r.result_title,
    l.source_version,
    l.raw_sha256,
    r.result_version,
    r.result_raw_sha256
from listed as l
left join days as d on l.sitting_id = d.sitting_id and l.day_number = d.day_number
left join results as r on l.vote_id = r.vote_id
