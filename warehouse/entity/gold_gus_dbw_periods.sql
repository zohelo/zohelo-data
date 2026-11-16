-- Step of the GUS DBW Gold build: the block's period ids (id_okres) placed on the calendar. DBW words a
-- period as `<cycle> - <kind of data> - <name>` ("miesiąc - dane miesięczne - styczeń", "kwartał - dane
-- kwartalne - 1 kwartał", "rok - dane roczne - rok", "co dwa lata - dane roczne - rok"); the year is the
-- row's id_daty. The frequency is the kind of data, the first month comes from the name:
--   dane roczne + rok → A; dane półroczne + N półrocze → S; dane kwartalne + N kwartał → Q;
--   dane miesięczne + month → M (also when the cycle is a year or a quarter: a monthly figure taken once a
--   year or once a quarter; the cycle then becomes part of the measure key, cycle_frequency).
-- Everything else has no frequency and its rows are not loaded (counted as explained exclusions,
-- docs/gold-coverage.md decision 6): cumulative periods ("dane narastające": January to the month, first
-- quarter to the quarter), three-year data ("dane trzyletnie") and years that are not calendar years
-- ("rok - dane roczne - lipiec - czerwiec"). They are never folded into another frequency.
with read as (
    select
        try_cast(element_id as integer) as period_id,
        label as period_name,
        lower(trim(string_split(label, ' - ')[1])) as cycle,
        lower(trim(string_split(label, ' - ')[2])) as kind,
        lower(trim(array_to_string(string_split(label, ' - ')[3:], ' - '))) as name
    from labels
    where column_name = 'id_okres'
),

months as (
    select * from (
        values
        ('styczeń', 1), ('luty', 2), ('marzec', 3), ('kwiecień', 4), ('maj', 5), ('czerwiec', 6),
        ('lipiec', 7), ('sierpień', 8), ('wrzesień', 9), ('październik', 10), ('listopad', 11), ('grudzień', 12)
    ) as t (month_name, month_number)
)

select
    r.period_id,
    r.period_name,
    case
        when r.cycle = 'miesiąc' then 'M'
        when r.cycle = 'kwartał' then 'Q'
        when r.cycle = 'półrocze' then 'S'
        when r.cycle = 'rok' or r.cycle like 'co %' or r.cycle = 'wielolecie' then 'A'
        else 'X'
    end as cycle_frequency,
    case
        when r.kind = 'dane roczne' and r.name = 'rok' then 'A'
        when r.kind = 'dane półroczne' and regexp_matches(r.name, '^[12] półrocze$') then 'S'
        when r.kind = 'dane kwartalne' and regexp_matches(r.name, '^[1-4] kwartał$') then 'Q'
        when r.kind = 'dane miesięczne' and m.month_number is not null then 'M'
    end as frequency,
    case
        when r.kind = 'dane roczne' and r.name = 'rok' then 1
        when r.kind = 'dane półroczne' and regexp_matches(r.name, '^[12] półrocze$')
            then (cast(substr(r.name, 1, 1) as integer) - 1) * 6 + 1
        when r.kind = 'dane kwartalne' and regexp_matches(r.name, '^[1-4] kwartał$')
            then (cast(substr(r.name, 1, 1) as integer) - 1) * 3 + 1
        when r.kind = 'dane miesięczne' then m.month_number
    end as first_month
from read as r
left join months as m on r.name = m.month_name
where r.period_id is not null
