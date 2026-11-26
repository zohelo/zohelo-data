-- Gold build stage of KNF's monthly banking-sector data (register PLG-017), by rule (docs/gold-coverage.md): every
-- value of silver.knf_banking_series, every sheet, item and bank group of the current workbook, in the common
-- observation shape (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the
-- table's files as one block. Relations:
--   silver      the data files of silver.knf_banking_series (sheet × bank group × item path × month or quarter end)
--   measure_key = knf_banking:knf_banking_series:<unit>:<sheet>:<item path>@<M|Q>: the sheet and the item path (the
--                 headings and parent labels above the item, joined with ' > ', as Silver builds it; never a row
--                 position) are one measure; frequency as Silver gives it per block (Q where every date of the
--                 block is a quarter end: capital adequacy, overdue receivables).
--   unit        = as published: a value under a '%' number format is a fraction as stored ('share', 0.215 =
--                 21.5%); every other value is in the unit the sheet names under 'Dane w' ('zł', 'sztukach'),
--                 else 'as named'. Nothing is rescaled (D33). The unit is part of the key, so a measure has one.
--   breakdown   = the bank group as published: {"bank_group": "<group label>"}; 'Sektor bankowy' (the whole
--                 sector) is the total. Commercial banks, branches of credit institutions and cooperative banks
--                 are parts of it, so a sum over groups double-counts.
--   date_key    = the first day of the month, or of the quarter for Q (the workbook dates each column by its last
--                 day); period_label 2026-09 or 2026-Q3, as the curated stage
--   geography   = Poland (PL): the workbook covers the Polish banking sector only
--   status      = `ok`: the workbook has no flags per cell
-- Not loaded: an empty cell (no Silver row; nothing is 0-filled) and a value that is not a finite number (Silver
-- keeps a non-numeric cell with value null and cell_status 'unparsed', which already fails its test).
-- The curated stage poc_build_knf_banking (seven items, codes of its own) stays the KPI layer.
with placed as (
    select
        s.*,
        case
            when contains(coalesce(s.number_format, ''), '%') then 'share'
            else coalesce(nullif(trim(s.unit_label), ''), 'as named')
        end as unit_code
    from silver as s
    where
        s.value is not null
        and not isnan(s.value)
        and not isinf(s.value)
        and s.cell_status = 'ok'
        and s.period_end is not null
)

select
    'knf_banking' as source_system,
    'knf_banking_series' as block_id,
    'KNF monthly banking-sector data, every sheet, item and bank group' as block_name,
    'knf_banking:knf_banking_series:' || p.unit_code || ':' || p.sheet_name || ':' || p.item_path || '@' || p.frequency
        as measure_key,
    p.sheet_name || ': ' || p.item_path as measure_name,
    p.sheet_name || ':' || p.item_path as measure_codes,
    p.unit_code,
    case p.unit_code when 'share' then 'share (1 = 100%), as published' else p.unit_code end as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    p.frequency,
    case
        when p.frequency = 'Q' then cast(date_trunc('quarter', p.period_end) as date)
        else cast(date_trunc('month', p.period_end) as date)
    end as date_key,
    case
        when p.frequency = 'Q' then year(p.period_end) || '-Q' || quarter(p.period_end)
        else strftime(p.period_end, '%Y-%m')
    end as period_label,
    '{"bank_group": ' || to_json(p.group_label) || '}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    p.group_label = 'Sektor bankowy' as is_total,
    p.value,
    'ok' as observation_status_key,
    p.source_version,
    p.raw_sha256
from placed as p
