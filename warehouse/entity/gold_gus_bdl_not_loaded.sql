-- Rows of ONE GUS BDL subgroup's Silver file that gold_gus_bdl_build.sql does not load, counted by the
-- reason (docs/gold-coverage.md, gate "Rows against Silver"): the Silver rows are the Gold rows plus
-- these. Same relations and steps as the build stage; the conditions are the build stage's.
select
    case
        when s.value is null then 'empty value'
        when s.value = 0 and z.sign is not null then 'no-value sign'
        else 'cumulative period'
    end as reason,
    count(*) as row_count
from silver as s
left join labels as l on s.dimension_values is not distinct from l.dimension_values
left join signs as z on s.channel = z.channel and lower(s.attribute) = z.sign
where
    s.value is null
    or coalesce(l.frequency, '') = 'X'
    or (s.value = 0 and z.sign is not null)
group by all
