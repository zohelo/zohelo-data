-- Gold build stage of one GUS BDL subgroup, step 2 (docs/gold-coverage.md): one row per variable of the
-- subgroup as its Silver file holds it (`dimension_values`: the labels n1…n5 as a JSON array), with what
-- the labels say. Kept as the table `labels` for the next steps, so every observation is shaped on its
-- own by one small join. The rule of poc_build_bdl.sql, read from the subgroup's own file:
--   period     = a first label that names a period ("styczeń" … "grudzień": monthly; "1 kwartał" …
--                "4 kwartał": quarterly; "rok": annual) is the period, not a breakdown, and the labels
--                shift left. A cumulative period ("1-3 kwartały", "styczeń-maj", "półrocze") cannot be
--                placed on the calendar: frequency X, not loaded, counted (gold_gus_bdl_not_loaded.sql).
--                Quarters are loaded beside months: the frequency is part of the measure key.
--   measure    = the label positions where no variable of the subgroup says "ogółem" name what is
--                measured ("urodzenia żywe", "ludność na 1 km2"); the other positions are breakdowns
--   total      = every breakdown label is "ogółem"
with held as (
    select distinct dimension_values
    from silver
),

parsed as (
    select
        dimension_values,
        coalesce(from_json(dimension_values, '["VARCHAR"]'), []) as all_labels
    from held
),

classified as (
    select
        *,
        case lower(trim(all_labels[1]))
            when 'styczeń' then 1 when 'luty' then 2 when 'marzec' then 3 when 'kwiecień' then 4
            when 'maj' then 5 when 'czerwiec' then 6 when 'lipiec' then 7 when 'sierpień' then 8
            when 'wrzesień' then 9 when 'październik' then 10 when 'listopad' then 11 when 'grudzień' then 12
        end as period_month,
        case
            when regexp_matches(lower(trim(all_labels[1])), '^[1-4] kwartał$')
                then cast(substr(trim(all_labels[1]), 1, 1) as integer)
        end as period_quarter,
        coalesce(lower(trim(all_labels[1])) = 'rok', false) as period_year,
        coalesce(
            regexp_matches(lower(trim(all_labels[1])), '^([1-4]-[1-4] kwartał|.*półrocz|styczeń-)'), false
        ) as period_cumulative
    from parsed
),

shifted as (  -- the period label (if any) is taken out; the labels shift left
    select
        dimension_values,
        case
            when period_month is not null or period_quarter is not null or period_year or period_cumulative
                then all_labels[1]
        end as period_code,
        case
            when period_month is not null then 'M'
            when period_quarter is not null then 'Q'
            when period_cumulative then 'X'
            else 'A'
        end as frequency,
        coalesce(period_month, (period_quarter - 1) * 3 + 1, 1) as start_month,
        case
            when period_month is not null or period_quarter is not null or period_year or period_cumulative
                then all_labels[2:]
            else all_labels
        end as labels
    from classified
),

positions as (
    select
        unnest(labels) as label,
        generate_subscripts(labels, 1) as pos
    from shifted
),

position_rule as (
    select
        pos,
        bool_and(lower(label) <> 'ogółem') as is_measure
    from positions
    where label is not null
    group by pos
),

measure_rule as (  -- one row: the positions that name the measure
    select coalesce(list(pos) filter (where is_measure), []) as measure_positions
    from position_rule
),

split as (
    select
        s.dimension_values,
        s.period_code,
        s.frequency,
        s.start_month,
        list_filter(
            list_transform(
                list_filter(
                    range(1, len(s.labels) + 1), lambda i: list_contains(r.measure_positions, i)  -- noqa: RF02
                ),
                lambda i: s.labels[i]  -- noqa: RF02
            ),
            lambda x: x is not null  -- noqa: RF02
        ) as measure_labels,
        list_filter(
            list_transform(
                list_filter(
                    range(1, len(s.labels) + 1), lambda i: not list_contains(r.measure_positions, i)  -- noqa: RF02
                ),
                lambda i: s.labels[i]  -- noqa: RF02
            ),
            lambda x: x is not null  -- noqa: RF02
        ) as breakdown_labels
    from shifted as s
    cross join measure_rule as r
)

select
    dimension_values,
    period_code,
    frequency,
    start_month,
    array_to_string(measure_labels, ' / ') as measure_label,
    cast(to_json(map(
        list_transform(range(1, len(breakdown_labels) + 1), lambda i: 'n' || i),  -- noqa: RF02
        breakdown_labels
    )) as varchar) as breakdown_json,
    case
        when list_contains(list_transform(breakdown_labels, lambda x: lower(x)), 'mężczyźni') then 'M'  -- noqa: RF02
        when list_contains(list_transform(breakdown_labels, lambda x: lower(x)), 'kobiety') then 'F'  -- noqa: RF02
        when list_contains(list_transform(breakdown_labels, lambda x: lower(x)), 'ogółem') then 'T'  -- noqa: RF02
    end as sex_code,
    len(list_filter(breakdown_labels, lambda x: lower(x) <> 'ogółem')) = 0 as is_total  -- noqa: RF02
from split
