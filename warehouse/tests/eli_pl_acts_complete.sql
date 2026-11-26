-- eli_pl (PLG-026): the acts of each journal match the count /eli/acts lists, within 0.5% (older years are read
-- again only every fourth week, so a few acts added to an old year can be missing until then); a journal year not
-- landed, or rows the decoder dropped, fail here. One test per Silver model (it reads no other model).
with acts as (
    select * from {{ ref('eli_pl_acts') }}
),

per_journal as (
    select
        publisher,
        count(*) as acts,
        max(acts_listed) as acts_listed
    from acts
    group by publisher
)

select
    publisher,
    acts,
    acts_listed,
    'count differs from /eli/acts by more than 0.5%' as problem
from per_journal
where acts_listed is null or abs(acts - acts_listed) > 0.005 * acts_listed
union all
select
    j.publisher,
    0 as acts,
    null as acts_listed,
    'journal has no acts' as problem
from (values ('DU'), ('MP')) as j (publisher)
where not exists (select 1 from acts as a where a.publisher = j.publisher)
