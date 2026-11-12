-- Rows of ONE GUS BDL subgroup's Silver file that gold_gus_bdl_build.sql does not load, counted by the
-- reason (docs/gold-coverage.md, gate "Rows against Silver"): the Silver rows are the Gold rows plus
-- these. Same relations as the build stage; the conditions are the build stage's, in its order.
select
    case
        when s.value is null then 'empty value'
        when
            s.value = 0
            and (
                (s.channel = 'web_export' and coalesce(lower(s.attribute), '') in ('x', 'n'))
                or (s.channel = 'api' and coalesce(s.attribute, '') in ('4', '14', '50', '91'))
            )
            then 'no-value sign'
        else 'cumulative period'
    end as reason,
    count(*) as row_count
from silver as s
left join labels as l on s.dimension_values is not distinct from l.dimension_values
where
    s.value is null
    or coalesce(l.frequency, '') = 'X'
    or (
        s.value = 0
        and (
            (s.channel = 'web_export' and coalesce(lower(s.attribute), '') in ('x', 'n'))
            or (s.channel = 'api' and coalesce(s.attribute, '') in ('4', '14', '50', '91'))
        )
    )
group by all
