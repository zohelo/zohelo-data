-- Gold build stage of the KNF monthly banking-sector data, by rule (docs/gold-coverage.md): every value of
-- silver.knf_banking_series, every item of every sheet and bank group of the current workbook, in the common
-- observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the
-- table's files as one block. Relations:
--   silver      the data files of silver.knf_banking_series (sheet × bank group × item path × period end)
--   measure_key = knf_banking:knf_banking_series:<unit>:<sheet>:<item path>@<M|Q>: the sheet and Silver's item path
--                 (headings and parent labels joined with ' > ', identified by label, never by row position) are the
--                 measure; an item whose path changes between workbooks is a new series. The frequency is the
--                 block's own (Silver: Q when every date of the block is a quarter end, else M) and ends the key, so
--                 a measure_key has one frequency.
--   unit        = as the workbook states it: a cell under a '%' number format holds a fraction as stored (0.215 is
--                 shown as 21.5%; unit share, never multiplied by 100); any other cell is in the unit written below
--                 'Dane w' on its sheet ('zł' -> pln, 'sztukach' -> number). A sheet without that line stays
--                 unit_code `as named` (unit_map does not map it; none in the workbook of 13 Nov 2026).
--   value       = as published (D33; nothing rescaled); year-to-date items (profit and loss) are never differenced
--   breakdown   = {"bank_group": <code>} for the block's bank group (banking_sector, commercial_banks,
--                 credit_institution_branches, cooperative_banks, commercial_banks_excl_associating; a label the list
--                 does not know keeps its text), label as published; banking_sector (the whole sector) is the total
--                 series (is_total)
--   date_key    = the first day of the month (M) or of the quarter (Q); period_label 2026-09 or 2026-Q3
--   geography   = Poland (PL): national figures
--   status      = `ok`: the workbook carries no flags
-- Not loaded: a value that is not a finite number (Silver keeps numbers only: an empty cell is no row, nothing is
-- 0-filled), a period end after the day of the download (source_version 20261126T091932Z; a projection, none today).
-- The curated stage poc_build_knf_banking (seven items, its own keys) stays the KPI layer.
with obs as (
    select
        s.*,
        case
            when contains(coalesce(s.number_format, ''), '%') then 'share'
            when s.unit_label = 'zł' then 'pln'
            when s.unit_label = 'sztukach' then 'number'
            else 'as named'
        end as unit_code
    from silver as s
    where
        s.value is not null
        and not isnan(s.value)
        and not isinf(s.value)
        and s.period_end is not null
        -- source_version is the download time, 20261126T091932Z (KNF sends no Last-Modified)
        and s.period_end <= coalesce(cast(try_strptime(left(s.source_version, 8), '%Y%m%d') as date), s.period_end)
)

select
    'knf_banking' as source_system,
    'knf_banking_series' as block_id,
    'KNF monthly banking-sector data, every item' as block_name,
    'knf_banking:knf_banking_series:' || o.unit_code || ':' || o.sheet_name || ':' || o.item_path || '@' || o.frequency
        as measure_key,
    o.item_path
    || ' ('
    || o.sheet_name
    || ', KNF, '
    || case o.unit_code when 'pln' then 'zł' when 'number' then 'number' when 'share' then 'share' else 'as named' end
    || case o.frequency when 'Q' then ', quarterly)' else ', monthly)' end as measure_name,
    o.sheet_name as measure_codes,
    o.unit_code,
    case o.unit_code
        when 'pln' then 'zł'
        when 'number' then 'w sztukach (number)'
        when 'share' then 'share (1 = 100%)'
        else coalesce(o.unit_label, 'as named')
    end as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    o.frequency,
    case when o.frequency = 'Q' then cast(date_trunc('quarter', o.period_end) as date) else o.period_date end
        as date_key,
    case
        when o.frequency = 'Q' then year(o.period_end) || '-Q' || quarter(o.period_end)
        else strftime(o.period_end, '%Y-%m')
    end as period_label,
    cast(to_json(map(['bank_group'], [
        case o.group_label
            when 'Sektor bankowy' then 'banking_sector'
            when 'Banki komercyjne' then 'commercial_banks'
            when 'Oddziały' then 'credit_institution_branches'
            when 'Banki spółdzielcze' then 'cooperative_banks'
            when 'Banki komercyjne bez zrzeszających' then 'commercial_banks_excl_associating'
            else o.group_label
        end
    ])) as varchar) as breakdown_json,
    cast(to_json(map(['bank_group'], [o.group_label])) as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    o.group_label = 'Sektor bankowy' as is_total,
    cast(o.value as double) as value,
    'ok' as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
