-- Gold build stage of NBP's balance of payments and international investment position, by rule
-- (docs/gold-coverage.md): every value of silver.nbp_bop_observations, the three workbooks (bop_m monthly, bop_q
-- quarterly, iip_q end of quarter) in all three currencies (PLN, EUR, USD), in the common observation shape (the
-- 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's files as one
-- block. Relations:
--   silver      the data files of silver.nbp_bop_observations (workbook × currency sheet × item × period)
--   measure_key = nbp_bop:nbp_bop_observations:mln_<currency>:<workbook>:<item code>@<M|Q>: workbook, currency and
--                 Silver's item code (fixed by rule from the English label and checked against the expected list
--                 by tests/nbp_bop_items.sql) are one measure; one frequency per workbook. IIP components and their
--                 parent totals are separate measures: a sum over measures double-counts.
--   unit        = million units of the sheet's currency, as published (mln_pln, mln_eur, mln_usd); currencies are
--                 never converted (D33), so each currency is its own measure
--   breakdown   = none ('{}', is_total)
--   date_key    = the first day of the month or quarter, as Silver places it; period_label as Silver
--   geography   = Poland (PL): the workbooks are Poland's accounts
--   status      = the curated stage's rule: the workbooks carry no flags and NBP revises the newest period with the
--                 next release, so the newest period of each workbook is `provisional`, every earlier one `ok`
-- Not loaded: a cell without a number or outside the period grid (Silver keeps it with value or date_key null,
-- which fails tests/nbp_bop_unplaced.sql) and a value that is not a finite number. Nothing is 0-filled.
-- The curated stage poc_build_nbp_bop (PLN and EUR, its own keys) stays the KPI layer; this adds the USD sheets.
with obs as (
    select
        s.*,
        max(s.date_key) over (partition by s.entity) as newest_date
    from silver as s
    where
        s.value is not null
        and not isnan(s.value)
        and not isinf(s.value)
        and s.date_key is not null
)

select
    'nbp_bop' as source_system,
    'nbp_bop_observations' as block_id,
    'NBP balance of payments and international investment position, every item and currency' as block_name,
    'nbp_bop:nbp_bop_observations:mln_' || lower(o.currency) || ':' || o.entity || ':' || o.item_code || '@' || o.frequency
        as measure_key,
    case o.entity
        when 'bop_m' then 'Balance of payments, monthly'
        when 'bop_q' then 'Balance of payments, quarterly'
        else 'International investment position, quarterly'
    end
    -- the English half of the bilingual label ("Rachunek bieżący / Current account"), else the item code
    || ' — ' || coalesce(nullif(trim(regexp_extract(o.item_label, '/\s*([^/]+)$', 1)), ''), o.item_code)
    || ' — million ' || o.currency as measure_name,
    o.entity || ':' || o.currency || ':' || o.item_code as measure_codes,
    'mln_' || lower(o.currency) as unit_code,
    'million ' || o.currency as unit_name,
    'PL' as geography_source_code,
    'PL' as geography_key,
    o.frequency,
    o.date_key,
    o.period_label,
    '{}' as breakdown_json,
    cast(null as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    o.value,
    case when o.date_key = o.newest_date then 'provisional' else 'ok' end as observation_status_key,
    o.source_version,
    o.raw_sha256
from obs as o
