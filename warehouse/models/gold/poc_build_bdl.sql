{{ config(alias='build_bdl') }}
{# Gold PoC build stage: sampled GUS BDL subgroups as observations in the common shape.
   Coarse measure rule (to refine per domain in v1): measure = subgroup × measure unit × the
   label positions where no variable of the subgroup says "ogółem" (those name what is
   measured: "urodzenia żywe", "ludność na 1 km2", "mieszkania"); the other positions are
   breakdowns; a series is total when every breakdown label is "ogółem".
   Short-term subgroups name the period in the first label ("styczeń" … "grudzień", "1 kwartał"
   … "4 kwartał"): that label is the period, not a breakdown, and the row is monthly or quarterly;
   the whole year ("rok") is an annual row; a cumulative period ("1-3 kwartały", "styczeń-maj",
   "półrocze") and the quarters of a subgroup that also has months are left unplaced (frequency X,
   counted by build_check). poc_bdl_measure_positions names label positions that define the measure
   although some variables say "ogółem" there (the market of the dwelling prices).
   Sample: years from 2010, units down to powiat (gmina for Ludność from 2015). #}
with blocks as ({{ poc_bdl_blocks() }}),

measure_positions as (
    select
        subgroup_id,
        list(position) as positions
    from ({{ poc_bdl_measure_positions() }})
    group by subgroup_id
),

geo_map as (
    select * from {{ ref('poc_map_geography') }}
    where source_system = 'gus_bdl'
),

units as (
    select
        unit_id,
        level,
        name as unit_name_of_id
    from {{ ref('bdl_units') }}
),

raw_variables as (
    select
        *,
        case lower(trim(cast(n1 as varchar)))
            when 'styczeń' then 1 when 'luty' then 2 when 'marzec' then 3 when 'kwiecień' then 4
            when 'maj' then 5 when 'czerwiec' then 6 when 'lipiec' then 7 when 'sierpień' then 8
            when 'wrzesień' then 9 when 'październik' then 10 when 'listopad' then 11 when 'grudzień' then 12
        end as period_month,
        case
            when regexp_matches(lower(trim(cast(n1 as varchar))), '^[1-4] kwartał$')
                then cast(substr(trim(cast(n1 as varchar)), 1, 1) as integer)
        end as period_quarter,
        lower(trim(cast(n1 as varchar))) = 'rok' as period_year,
        regexp_matches(lower(trim(cast(n1 as varchar))), '^([1-4]-[1-4] kwartał|.*półrocz|styczeń-)')
            as period_cumulative
    from {{ ref('bdl_variables') }}
),

periods as (
    select
        *,
        period_month is not null
        or period_quarter is not null
        or coalesce(period_year, false)
        or coalesce(period_cumulative, false) as is_period
    from raw_variables
),

monthly_subgroups as (  -- a subgroup with months: its quarters are averages of them, not placed
    select
        subgroup_id,
        bool_or(period_month is not null) as has_month
    from raw_variables
    group by subgroup_id
),

variables as (  -- the period label (if any) is taken out of n1; the labels shift left
    select
        v.subgroup_id,
        v.variable_id,
        case when v.is_period then cast(v.n1 as varchar) end as period_code,
        case
            when not v.is_period or v.period_year then 'A'
            when v.period_month is not null then 'M'
            when v.period_quarter is not null and not s.has_month then 'Q'
            else 'X'
        end as period_frequency,
        coalesce(v.period_month, (v.period_quarter - 1) * 3 + 1) as period_start_month,
        case when v.is_period then v.n2 else v.n1 end as n1,
        case when v.is_period then v.n3 else v.n2 end as n2,
        case when v.is_period then v.n4 else v.n3 end as n3,
        case when v.is_period then v.n5 else v.n4 end as n4,
        case when v.is_period then null else v.n5 end as n5
    from periods as v
    inner join monthly_subgroups as s on v.subgroup_id = s.subgroup_id
),

raw_label_rule as (  -- a label position names what is measured when no variable of the subgroup has "ogółem" there
    select
        subgroup_id,
        bool_and(lower(cast(n1 as varchar)) <> 'ogółem') filter (where n1 is not null) as m1,
        bool_and(lower(cast(n2 as varchar)) <> 'ogółem') filter (where n2 is not null) as m2,
        bool_and(lower(cast(n3 as varchar)) <> 'ogółem') filter (where n3 is not null) as m3,
        bool_and(lower(cast(n4 as varchar)) <> 'ogółem') filter (where n4 is not null) as m4,
        bool_and(lower(cast(n5 as varchar)) <> 'ogółem') filter (where n5 is not null) as m5
    from variables
    group by subgroup_id
),

label_rule as (  -- plus the positions named in poc_bdl_measure_positions
    select
        r.subgroup_id,
        coalesce(r.m1, false) or coalesce(list_contains(p.positions, 1), false) as m1,
        coalesce(r.m2, false) or coalesce(list_contains(p.positions, 2), false) as m2,
        coalesce(r.m3, false) or coalesce(list_contains(p.positions, 3), false) as m3,
        coalesce(r.m4, false) or coalesce(list_contains(p.positions, 4), false) as m4,
        coalesce(r.m5, false) or coalesce(list_contains(p.positions, 5), false) as m5
    from raw_label_rule as r
    left join measure_positions as p on r.subgroup_id = p.subgroup_id
),

silver as (
    select
        o.*,
        b.block_name,
        b.subject_path,
        u.unit_name_of_id,
        coalesce(
            u.level,
            case  -- a 7-digit web-export code: 0000000 Polska, WW00000 województwo, WWPP000 powiat, else gmina
                when o.unit_code = '0000000' then 0
                when regexp_matches(o.unit_code, '^[0-9]{2}00000$') then 2
                when regexp_matches(o.unit_code, '^[0-9]{4}000$') then 5
                else 6
            end
        ) as unit_level,
        v.n1,
        v.n2,
        v.n3,
        v.n4,
        v.n5,
        coalesce(v.period_frequency, 'A') as frequency,
        v.period_code,
        case coalesce(v.period_frequency, 'A')
            when 'A' then make_date(o.year, 1, 1)
            when 'X' then null
            else make_date(o.year, v.period_start_month, 1)
        end as date_key,
        array_to_string(
            list_filter(
                [
                    case when coalesce(r.m1, false) then cast(v.n1 as varchar) end,
                    case when coalesce(r.m2, false) then cast(v.n2 as varchar) end,
                    case when coalesce(r.m3, false) then cast(v.n3 as varchar) end,
                    case when coalesce(r.m4, false) then cast(v.n4 as varchar) end,
                    case when coalesce(r.m5, false) then cast(v.n5 as varchar) end,
                    -- a whole-year row ("rok") of a short-term subgroup is a measure of its own: the series key has no
                    -- frequency, so it would share 1 January with the first quarter (tests/fact_grain.sql)
                    case when v.period_frequency = 'A' and v.period_code is not null then 'rok' end
                ],
                lambda x: x is not null  -- noqa: RF02
            ),
            ' / '
        ) as measure_label,
        list_filter(
            [
                case when coalesce(r.m1, false) then null else cast(v.n1 as varchar) end,
                case when coalesce(r.m2, false) then null else cast(v.n2 as varchar) end,
                case when coalesce(r.m3, false) then null else cast(v.n3 as varchar) end,
                case when coalesce(r.m4, false) then null else cast(v.n4 as varchar) end,
                case when coalesce(r.m5, false) then null else cast(v.n5 as varchar) end
            ],
            lambda x: x is not null  -- noqa: RF02
        ) as labels
    from {{ source('silver_entities', 'bdl_observations') }} as o
    inner join blocks as b on o.subgroup_id = b.subgroup_id
    left join units as u on o.unit_code = u.unit_id
    left join variables as v on o.subgroup_id = v.subgroup_id and o.variable_id = v.variable_id
    left join label_rule as r on o.subgroup_id = r.subgroup_id
    where
        o.value is not null and o.year >= 2010
        -- a cell the source marks and stores as 0 is no value: website symbol "x" (not applicable or not
        -- published) or "a", API attributes 0, 4, 91 (#2743); "z" (less than half a unit) is a real rounded value
        and not (o.attribute is not null and o.value = 0 and o.attribute <> 'z')
),

kept as (
    select *
    from silver
    where
        -- a BDL "region" (level 3) that is a whole województwo duplicates the level-2 unit; only
        -- Mazowieckie's two NUTS 2 regions are areas of their own
        not (
            unit_level = 3
            and upper(unit_name) not like '%WARSZAWSKI STO%'
            and upper(unit_name) not like '%MAZOWIECKI REGIONALNY%'
        )
        -- the "makroregion województwo mazowieckie" (level 1) is the voivodeship itself (level 2)
        and not (unit_level = 1 and upper(unit_name) like '%MAZOWIECKIE%')
        and case subgroup_id
            when 'P2137'
                then
                    (unit_level <= 5 or year >= 2015)
                    and len(list_filter(labels, lambda x: lower(x) not in ('ogółem', 'mężczyźni', 'kobiety'))) = 0
            -- zohelo-bi first slice (O31, #2592): Poland, makroregions and voivodeships only
            when 'P2955' then unit_level <= 2
            when 'P2496' then unit_level <= 2
            when 'P4635' then unit_level <= 2
            when 'P2359' then unit_level <= 2
            when 'P2360' then unit_level <= 2
            when 'P2361' then unit_level <= 2
            when 'P2961' then unit_level <= 2
            when 'P4334' then unit_level <= 2
            when 'P2730' then unit_level <= 2
            when 'P2346' then unit_level <= 2
            when 'P2426' then unit_level <= 2
            when 'P1869' then unit_level <= 2
            when 'P2861' then unit_level <= 2
            when 'P2860' then unit_level <= 2
            when 'P2862' then unit_level <= 2
            when 'P3502' then unit_level <= 2
            -- slice needs-block rows (O36, #2743)
            when 'P2656' then unit_level <= 2
            when 'P2713' then unit_level <= 2
            when 'P2692' then unit_level <= 2
            when 'P2721' then unit_level <= 2
            when 'P4610' then unit_level <= 2
            when 'P3788' then unit_level <= 2
            when 'P3794' then unit_level <= 2
            -- 130,000 rows by NACE section: the all-sections total only
            when 'P2837'
                then unit_level <= 2 and len(list_filter(labels, lambda x: lower(x) <> 'ogółem')) = 0
            else unit_level <= 5
        end
),

rebased as (  -- a measure published in several price bases ("średnie ceny bieżące z 2015 r.") is chained:
    -- per period the newest base, one measure without the base in its label (the bases overlap, #2743)
    select * exclude (price_base, base_free_label)
    from (
        select
            * replace (
                regexp_replace(measure_label, ' \(średnie ceny bieżące z [0-9]{4} r\.\)', '') as measure_label
            ),
            try_cast(regexp_extract(measure_label, 'z ([0-9]{4}) r\.\)', 1) as integer) as price_base,
            regexp_replace(measure_label, ' \(średnie ceny bieżące z [0-9]{4} r\.\)', '') as base_free_label
        from kept
    ) as based
    qualify
        price_base is null
        or price_base = max(price_base) over (
            partition by subgroup_id, unit_code, unit_name, measure_unit, base_free_label, labels, year, period_code
        )
),

mapped as (
    select
        k.*,
        -- a row whose unit name disagrees with the name of its id is another unit under a borrowed code
        -- (the web export gives the Górnośląsko-Zagłębiowska Metropolia the code of Śląskie): unmatched
        case
            when k.unit_name_of_id is not null and upper(trim(k.unit_name_of_id)) <> upper(trim(k.unit_name)) then '_X'
            else coalesce(g.geography_key, '_X')
        end as geography_key
    from rebased as k
    left join geo_map as g on k.unit_code = g.source_code
),

deduplicated as (  -- one row per series × area × year: a subgroup can hold the same series under its
    -- website code and its API id (Silver finding #868); the API row wins, then the newest version
    select *
    from mapped
    qualify row_number() over (
        partition by
            subgroup_id, measure_unit, measure_label, labels,
            case when geography_key = '_X' then '_X:' || unit_code else geography_key end,  -- the series identity
            year, period_code
        order by case when channel = 'api' then 0 else 1 end, source_version desc, unit_code asc
    ) = 1
)

select
    'gus_bdl' as source_system,
    k.subgroup_id as block_id,
    k.block_name,
    'gus_bdl:'
    || k.subgroup_id
    || ':'
    || coalesce(k.measure_unit, '-')
    || coalesce(':' || nullif(k.measure_label, ''), '') as measure_key,
    k.block_name
    || coalesce(' — ' || nullif(k.measure_label, ''), '')
    || ' — '
    || coalesce(k.measure_unit, '-') as measure_name,
    coalesce(k.measure_unit, '-') || coalesce(':' || nullif(k.measure_label, ''), '') as measure_codes,
    coalesce(k.measure_unit, '-') as unit_code,
    coalesce(k.measure_unit, '-') as unit_name,
    k.unit_code as geography_source_code,
    k.geography_key,
    k.frequency,
    k.date_key,
    coalesce(k.period_code || ' ', '') || k.year as period_label,
    cast(to_json(map(
        list_transform(generate_series(1, len(k.labels)), lambda i: 'n' || i),  -- noqa: RF03
        k.labels
    )) as varchar) as breakdown_json,
    cast(to_json(map(
        list_transform(generate_series(1, len(k.labels)), lambda i: 'n' || i),  -- noqa: RF03
        k.labels
    )) as varchar) as breakdown_label_json,
    case
        when list_contains(list_transform(k.labels, lambda x: lower(x)), 'mężczyźni') then 'M'  -- noqa: RF03
        when list_contains(list_transform(k.labels, lambda x: lower(x)), 'kobiety') then 'F'  -- noqa: RF03
        when list_contains(list_transform(k.labels, lambda x: lower(x)), 'ogółem') then 'T'  -- noqa: RF03
    end as sex_code,
    len(list_filter(k.labels, lambda x: lower(x) <> 'ogółem')) = 0 as is_total,  -- noqa: RF03
    k.value,
    case
        when k.value_conflict then 'conflicting_values'
        when k.attribute = 'z' then 'not_significant'  -- less than half a unit, published as 0
        when k.attribute is not null then 'flagged'
        else 'ok'
    end as observation_status_key,
    k.source_version,
    k.raw_sha256
from deduplicated as k
