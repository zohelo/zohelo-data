-- Gold build stage of the Ministry of Finance public debt workbook, by rule (docs/gold-coverage.md): every value of
-- silver.public_debt, every sheet, block and item of the workbook (public finance sector debt before and after
-- consolidation by subsector, by instrument, by place of issue and residency, general government (EDP) debt by ESA 2010
-- instrument, the PDP-EDP bridge, debt in relation to GDP), in the common observation shape (the 21 columns of
-- poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's files as one block. The
-- workbook's figures are stocks at the end of a period (debt outstanding, a debt-to-GDP ratio at that date).
-- Relations:
--   silver      the data files of silver.public_debt (sheet × block × item × period end, newest workbook)
--   measure_key = mf_public_debt:public_debt:<unit>:<sheet>:<block title>[#<n>]:<item label>[#<occurrence>]@<A|Q|M>:
--                 a block is a header row of a sheet; its title (the nearest text above the header, else the sheet
--                 name) gets #<n> only when a sheet has n > 1 blocks of that title (in header order); an item label
--                 repeated in a block (the same debts in million zloty and as a fraction of GDP) gets its occurrence
--                 when above 1. Every label as published.
--   frequency   = one per block, from the months of its period ends: only December A, only quarter-end months Q,
--                 otherwise M. A stock at 31 December is the stock at the end of that quarter or month, so the
--                 year ends before the quarterly or monthly part of a block are points of the block's frequency.
--   unit        = Silver's unit as published (D33, nothing rescaled): mln_pln (million zloty) or ratio (a
--                 '%'-formatted cell, a fraction: 0.55 = 55 %; the curated stage's x100 is not applied here)
--   breakdown   = none ('{}', is_total)
--   date_key    = the first day of the period that ends at the period end; period_label 2025, 2025-Q3, 2025-09
--   geography   = Poland (PL): the workbook is Poland's public debt
--   status      = `ok`: the workbook carries no flags
-- Not loaded: a value that is not a finite number (Silver keeps numbers only; nothing is 0-filled), a period end
-- after the day of the workbook's version (a projection; none expected) or a version that cannot be read, and two
-- period ends of one measure in one period (never a pick; none expected: a header holds a date once).
-- The curated stage poc_build_public_finance (three sheets, its own keys) stays the KPI layer.
with obs as (
    select s.*
    from silver as s
    where
        s.value is not null
        and not isnan(s.value)
        and not isinf(s.value)
        -- a version that cannot be read loads nothing rather than a period that may not have ended
        and s.period_end <= try_strptime(left(s.source_version, 8), '%Y%m%d')
),

block_dates as (
    select distinct
        sheet_name,
        header_row,
        coalesce(block_title, sheet_name) as block_title,
        period_end
    from obs
),

blocks as (
    select
        sheet_name,
        header_row,
        block_title,
        case
            when bool_and(month(period_end) = 12) then 'A'
            when bool_and(month(period_end) in (3, 6, 9, 12)) then 'Q'
            else 'M'
        end as frequency,
        dense_rank() over (partition by sheet_name, block_title order by header_row) as title_rank,
        count(*) over (partition by sheet_name, block_title) as title_blocks
    from block_dates
    group by sheet_name, header_row, block_title
),

placed as (
    select
        o.*,
        b.frequency,
        o.unit as unit_code,
        b.block_title
        || case when b.title_blocks > 1 then '#' || b.title_rank else '' end as block_code,
        o.item_label || case when o.item_occurrence > 1 then '#' || o.item_occurrence else '' end as item_code_rule,
        case b.frequency
            when 'A' then make_date(year(o.period_end), 1, 1)
            when 'Q' then make_date(year(o.period_end), 3 * quarter(o.period_end) - 2, 1)
            else make_date(year(o.period_end), month(o.period_end), 1)
        end as date_key,
        case b.frequency
            when 'A' then cast(year(o.period_end) as varchar)
            when 'Q' then year(o.period_end) || '-Q' || quarter(o.period_end)
            else strftime(o.period_end, '%Y-%m')
        end as period_label
    from obs as o
    inner join blocks as b on o.sheet_name = b.sheet_name and o.header_row = b.header_row
),

keyed as (
    select
        *,
        'mf_public_debt:public_debt:' || unit_code || ':' || sheet_name || ':' || block_code || ':' || item_code_rule
        || '@' || frequency as measure_key
    from placed
)

select
    'mf_public_debt' as source_system,
    'public_debt' as block_id,
    'Ministry of Finance public debt workbook, every sheet and item' as block_name,
    k.measure_key,
    k.sheet_name || ' — ' || k.block_code || ' — ' || k.item_code_rule
    || case k.unit_code when 'ratio' then ' (fraction)' else ' (million zloty)' end as measure_name,
    coalesce(k.item_code, k.item_code_rule) as measure_codes,
    k.unit_code,
    case k.unit_code when 'ratio' then 'share (1 = 100%)' else 'million zloty' end as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    k.frequency,
    k.date_key,
    k.period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(k.value as double) as value,
    'ok' as observation_status_key,
    k.source_version,
    k.raw_sha256
from keyed as k
-- two period ends of one measure in one period: both stay out, none is picked
qualify count(*) over (partition by k.measure_key, k.date_key) = 1
