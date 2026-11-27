-- senat (PLG-025): the votes of every sitting are numbered 1..n without a gap (a day page not read or a vote row the
-- parser dropped fails here), and a vote whose page is read has every total, with for + against + abstained = voted.
-- One test per Silver model (it reads no other model).
with votes as (
    select * from {{ ref('senat_votes') }}
),

per_sitting as (
    select
        sitting_id,
        count(*) as votes,
        min(vote_number) as first_number,
        max(vote_number) as last_number
    from votes
    group by sitting_id
)

select
    sitting_id,
    null as vote_id,
    'vote numbers have a gap' as problem
from per_sitting
where first_number <> 1 or last_number <> votes
union all
select
    sitting_id,
    vote_id,
    'totals incomplete or inconsistent' as problem
from votes
where
    results_landed
    and (
        voted is null or votes_for is null or votes_against is null or abstained is null
        or votes_for + votes_against + abstained <> voted
    )
union all
select
    null as sitting_id,
    null as vote_id,
    'no votes' as problem
where not exists (select 1 from votes)
