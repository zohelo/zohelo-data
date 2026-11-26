-- Rows of ONE GUS DBW block's Silver files that gold_gus_dbw_build.sql does not load, counted by the reason
-- (docs/gold-coverage.md, gate "Rows against Silver"): the Silver rows are the Gold rows plus these. Same
-- relations and preparation steps as the build stage (periods, twins); the conditions are the build stage's,
-- checked in this order:
--   empty value                 wartosc is empty
--   zero with a no-value sign   0 with id_brak_wartosci other than 253 "Nie dotyczy"
--   confidential zero           0 with id_tajnosci other than 43 "Dane jawne"
--   period not on the calendar  a cumulative, three-year or non-calendar-year period, or a year outside
--                               1000 to 9999 (decision 5)
--   twin of a voivodeship       a NUTS twin row equal to its voivodeship's row (gold_gus_dbw_twins.sql)
select
    case
        when s.value is null then 'empty value'
        when s.value = 0 and coalesce(s.missing_reason_id, 253) <> 253 then 'zero with a no-value sign'
        when s.value = 0 and coalesce(s.confidentiality_id, 43) <> 43 then 'confidential zero'
        when pe.frequency is null or s.year is null or s.year not between 1000 and 9999
            then 'period not on the calendar'
        else 'twin of a voivodeship'
    end as reason,
    count(*) as row_count
from silver as s
left join periods as pe on s.period_id = pe.period_id
left join twins as t
    on
        s.entity_id = t.entity_id
        and s.presentation_id = t.presentation_id
        and s.period_id = t.period_id
        and s.year = t.year
        and [
            s.position_1_id,
            s.position_2_id,
            s.position_3_id,
            s.position_4_id,
            s.position_5_id,
            s.position_6_id,
            s.position_7_id,
            s.position_8_id,
            s.position_9_id
        ]
        = t.positions
where
    s.value is null
    or (s.value = 0 and (coalesce(s.missing_reason_id, 253) <> 253 or coalesce(s.confidentiality_id, 43) <> 43))
    or pe.frequency is null
    or s.year is null
    or s.year not between 1000 and 9999
    or t.entity_id is not null
group by all
