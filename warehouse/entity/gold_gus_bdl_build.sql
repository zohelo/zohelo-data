-- Gold build stage of ONE GUS BDL subgroup, by rule (docs/gold-coverage.md): every row of the subgroup's
-- Silver file with a value, every year and every territorial level, in the common observation shape (the
-- 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to one Silver file,
-- after the two steps that make the tables `labels` (gold_gus_bdl_labels.sql: period, measure and total
-- rules) and `units` (gold_gus_bdl_units.sql: the area of each unit). Relations:
--   silver    the subgroup's file of silver.bdl_observations
--   names     gold.coverage rows of GUS BDL (block_id, block_name)
-- Every row is shaped on its own with these small lookups: nothing is grouped or ranked by series.
--   measure_key = gus_bdl:<subgroup>:<measure unit>[:<measure labels>]@<frequency>
--   date_key    = 1 January of the year, or the first day of the month or quarter the first label names
--   unit        = the measure unit as published (D33); nothing is converted
--   no value    = a 0 under a no-value sign is not a value and is not loaded (#2788, #2806, #2852): website
--                 signs x, X ("brak informacji, konieczność zachowania tajemnicy statystycznej lub
--                 wypełnienie pozycji jest niemożliwe albo niecelowe") and n ("dana jeszcze niedostępna");
--                 API attribute ids 4 and 91 (x), 14 (X), 50 (n). A number other than 0 under such a
--                 sign is kept. Kept too: z/Z (a significant zero), a/A and the API's id 0 (blank sign).
--   status      = `forecast` for a year after the year of the subgroup's version (a value for a year
--                 that had not ended when GUS published it is not an observation) and for every row of a
--                 subgroup named a projection ("prognoza"); else `ok`
with block_names as (
    select
        block_id,
        any_value(block_name) as block_name
    from names
    group by block_id
)

select
    'gus_bdl' as source_system,
    s.subgroup_id as block_id,
    coalesce(n.block_name, s.subgroup_id) as block_name,
    'gus_bdl:'
    || s.subgroup_id
    || ':'
    || coalesce(s.measure_unit, '-')
    || coalesce(':' || nullif(l.measure_label, ''), '')
    || '@'
    || l.frequency as measure_key,
    coalesce(n.block_name, s.subgroup_id)
    || coalesce(' — ' || nullif(l.measure_label, ''), '')
    || ' — '
    || coalesce(s.measure_unit, '-')
    || ' ['
    || l.frequency
    || ']' as measure_name,
    l.measure_label as measure_codes,
    coalesce(s.measure_unit, '-') as unit_code,
    coalesce(s.measure_unit, '-') as unit_name,
    u.geography_source_code,
    u.geography_key,
    l.frequency,
    make_date(s.year, l.start_month, 1) as date_key,
    coalesce(l.period_code || ' ', '') || s.year as period_label,
    l.breakdown_json,
    -- the breakdown values are the labels themselves: not repeated in a second column
    cast(null as varchar) as breakdown_label_json,
    l.sex_code,
    l.is_total,
    s.value,
    case
        when s.value_conflict then 'conflicting_values'
        when lower(n.block_name) like '%prognoz%' then 'forecast'
        when
            substr(s.source_version, 1, 4) <> '1970'
            and s.year > try_cast(substr(s.source_version, 1, 4) as integer)
            then 'forecast'
        else 'ok'
    end as observation_status_key,
    s.source_version,
    s.raw_sha256
from silver as s
left join labels as l on s.dimension_values is not distinct from l.dimension_values
left join units as u on s.unit_code = u.unit_code and s.unit_name is not distinct from u.unit_name
left join block_names as n on s.subgroup_id = n.block_id
where
    s.value is not null
    and coalesce(l.frequency, '') <> 'X'
    and not (
        s.value = 0
        and (
            (s.channel = 'web_export' and coalesce(lower(s.attribute), '') in ('x', 'n'))
            or (s.channel = 'api' and coalesce(s.attribute, '') in ('4', '14', '50', '91'))
        )
    )
