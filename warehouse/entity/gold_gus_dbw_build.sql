-- Gold build stage of ONE GUS DBW block, by rule (docs/gold-coverage.md): every row of the block's Silver
-- files with a value, in the common observation shape (the 21 columns of poc_build_nbp.sql, same order).
-- A block is indicator × cross-section × presentation × periodicity; its Silver files are one per year (or
-- one file). Applied by warehouse/observations.py after the steps that make the small tables it reads:
--   labels (gold_gus_dbw_labels.sql)    the block's dictionary: names of its ids
--   periods (gold_gus_dbw_periods.sql)  period id → frequency and first month
--   areas, shape (gold_gus_dbw_areas.sql, gold_gus_dbw_shape.sql)  the territorial slot, totals, sex
--   twins (gold_gus_dbw_twins.sql)      rows of a NUTS twin that repeat a voivodeship row
-- Every row is shaped on its own with these small tables: nothing is grouped by series.
--   block_id    = the file id without language and year (the same rule as observations.py dbw_block)
--   measure_key = gus_dbw:<block>:<presentation id>[:<cycle>]@<frequency>: indicator × presentation, as the
--                 curated poc_build_dbw, plus the frequency (series_key has none); <cycle> only where a
--                 monthly figure is taken once a quarter (Q) or once a year (A)
--   breakdown   = the other dimensions, DBW's own ids as published: {"<dimension id>":"<position id>"};
--                 their names are in the block's dictionary, not repeated per row (decision 7)
--   unit        = the presentation's name as published (D33), the key of unit_map.csv
--   date_key    = first day of the period (A, S, Q, M); other periods are not loaded
--   geography   = the position of the territorial slot, by name through gold.map_geography; a block without
--                 one → _Z; unmatched → _X with `<NAME> #<position id>` kept in the key
-- Cells that are not plain values (DBW's id_brak_wartosci, id_tajnosci, id_flaga):
--   empty value                          not loaded
--   0 with a no-value sign               not loaded: DBW writes 0 with 42 "zjawisko nie wystąpiło", 38 "brak
--                                        informacji, konieczność zachowania tajemnicy…", 725 "wartość mniejsza
--                                        niż przyjęty format prezentacji"; 253 "Nie dotyczy" marks a real value
--   non-zero with a no-value sign        loaded, status `flagged` (a number is never dropped)
--   id_tajnosci other than 43 "Dane jawne"   loaded, status `confidential`
--   id_flaga     31, 308 → provisional; 29, 35 → estimated; 30 "Prognoza" → forecast; 1 → break;
--                251 → low_reliability; 36, 346, 33, 32, 305 (not applicable, final, full population,
--                revised) → ok; any other flag → flagged
with flags as (
    select * from (
        values
        (31, 'provisional'), (308, 'provisional'), (29, 'estimated'), (35, 'estimated'), (30, 'forecast'),
        (1, 'break'), (251, 'low_reliability'), (36, 'ok'), (346, 'ok'), (33, 'ok'), (32, 'ok'), (305, 'ok')
    ) as t (flag_id, observation_status_key)
),

placed as (
    select
        s.*,
        regexp_replace(
            regexp_replace(s.entity_id, '^([0-9]+_[0-9]+_[0-9]+)_[0-9]{4}(_[0-9]{1,3})_pl$', '\1\2_pl'),
            '(_[0-9]{4})?_pl$',
            ''
        ) as block_id,
        pe.frequency,
        pe.cycle_frequency,
        pe.period_name,
        make_date(s.year, pe.first_month, 1) as date_key,
        [
            s.position_1_id,
            s.position_2_id,
            s.position_3_id,
            s.position_4_id,
            s.position_5_id,
            s.position_6_id,
            s.position_7_id,
            s.position_8_id,
            s.position_9_id
        ] as positions,
        case sh.territory_slot
            when 1 then s.position_1_id
            when 2 then s.position_2_id
            when 3 then s.position_3_id
            when 4 then s.position_4_id
            when 5 then s.position_5_id
            when 6 then s.position_6_id
            when 7 then s.position_7_id
            when 8 then s.position_8_id
            when 9 then s.position_9_id
        end as area_position_id,
        sh.territory_slot,
        sh.totals,
        sh.men,
        sh.women,
        sh.sex_totals,
        list_filter(
            [
                { 'slot': 1, 'dimension_id': s.dimension_1_id, 'position_id': s.position_1_id },
                { 'slot': 2, 'dimension_id': s.dimension_2_id, 'position_id': s.position_2_id },
                { 'slot': 3, 'dimension_id': s.dimension_3_id, 'position_id': s.position_3_id },
                { 'slot': 4, 'dimension_id': s.dimension_4_id, 'position_id': s.position_4_id },
                { 'slot': 5, 'dimension_id': s.dimension_5_id, 'position_id': s.position_5_id },
                { 'slot': 6, 'dimension_id': s.dimension_6_id, 'position_id': s.position_6_id },
                { 'slot': 7, 'dimension_id': s.dimension_7_id, 'position_id': s.position_7_id },
                { 'slot': 8, 'dimension_id': s.dimension_8_id, 'position_id': s.position_8_id },
                { 'slot': 9, 'dimension_id': s.dimension_9_id, 'position_id': s.position_9_id }
            ],
            lambda x: x.position_id is not null and x.slot is distinct from sh.territory_slot  -- noqa: RF01
        ) as breakdown
    from silver as s
    cross join shape as sh
    inner join periods as pe on s.period_id = pe.period_id
    where
        s.value is not null and (coalesce(s.missing_reason_id, 253) = 253 or s.value <> 0)
        and pe.frequency is not null
        and s.year is not null
),

kept as (
    select p.*
    from placed as p
    where
        not exists (
            select 1
            from twins as t
            where
                p.entity_id = t.entity_id
                and p.presentation_id = t.presentation_id
                and p.period_id = t.period_id
                and p.year = t.year
                and p.positions = t.positions
        )
),

shaped as (
    select
        p.*,
        coalesce(nullif(trim(i.label), ''), 'indicator ' || cast(p.indicator_id as varchar)) as indicator_name,
        coalesce(nullif(trim(x.label), ''), 'cross-section ' || cast(p.section_id as varchar)) as section_name,
        coalesce(nullif(trim(m.label), ''), cast(p.presentation_id as varchar)) as presentation_name,
        cast(p.presentation_id as varchar)
        || case when p.cycle_frequency = p.frequency then '' else ':' || p.cycle_frequency end as measure_codes,
        list_transform(p.breakdown, lambda x: cast(x.slot as varchar) || ':' || cast(x.position_id as varchar))
            as slot_positions
    from kept as p
    left join labels as i
        on i.column_name = 'id_zmienna' and cast(p.indicator_id as varchar) = i.element_id
    left join labels as x
        on x.column_name = 'id_przekroj' and cast(p.section_id as varchar) = x.element_id
    left join labels as m
        on m.column_name = 'id_sposob_prezentacji_miara' and cast(p.presentation_id as varchar) = m.element_id
)

select
    'gus_dbw' as source_system,
    p.block_id,
    p.indicator_name || ' — ' || p.section_name as block_name,
    'gus_dbw:' || p.block_id || ':' || p.measure_codes || '@' || p.frequency as measure_key,
    p.indicator_name
    || ' — '
    || p.presentation_name
    || case
        when p.cycle_frequency = p.frequency then ''
        when p.cycle_frequency = 'Q' then ' — once a quarter'
        when p.cycle_frequency = 'A' then ' — once a year'
        else ' — cycle ' || p.cycle_frequency
    end
    || ' ['
    || p.frequency
    || ']' as measure_name,
    p.measure_codes,
    p.presentation_name as unit_code,
    p.presentation_name as unit_name,
    case
        when p.area_position_id is null then null
        else coalesce(a.geography_source_code, ' #' || cast(p.area_position_id as varchar))
    end as geography_source_code,
    case when p.area_position_id is null then '_Z' else coalesce(a.geography_key, '_X') end as geography_key,
    p.frequency,
    p.date_key,
    coalesce(p.period_name || ' ', '') || cast(p.year as varchar) as period_label,
    '{'
    || array_to_string(
        list_transform(
            p.breakdown,
            lambda x: '"'
            || coalesce(cast(x.dimension_id as varchar), 'slot' || cast(x.slot as varchar))  -- noqa: RF01
            || '":"'
            || cast(x.position_id as varchar)  -- noqa: RF01
            || '"'
        ),
        ','
    )
    || '}' as breakdown_json,
    -- names of the positions are not repeated on every row: the block's dictionary names them
    cast(null as varchar) as breakdown_label_json,
    case
        when list_has_any(p.slot_positions, p.men) then 'M'
        when list_has_any(p.slot_positions, p.women) then 'F'
        when list_has_any(p.slot_positions, p.sex_totals) then 'T'
    end as sex_code,
    len(list_filter(p.slot_positions, lambda x: not list_contains(p.totals, x))) = 0 as is_total,  -- noqa: RF02
    p.value,
    case
        when p.confidentiality_id is not null and p.confidentiality_id <> 43 then 'confidential'
        when coalesce(p.missing_reason_id, 253) <> 253 then 'flagged'
        when p.flag_id is null then 'ok'
        else coalesce(f.observation_status_key, 'flagged')
    end as observation_status_key,
    p.source_version,
    p.raw_sha256
from shaped as p
left join areas as a on p.area_position_id = a.position_id
left join flags as f on p.flag_id = f.flag_id
