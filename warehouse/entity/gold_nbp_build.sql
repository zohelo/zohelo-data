-- Gold build stage of the NBP exchange rate tables, by rule (docs/gold-coverage.md): every rate of
-- silver.nbp_fx_rates (tables A, B and C, every currency, every date), in the common observation shape
-- (the 21 columns of poc_build_nbp.sql, same order). Applied by warehouse/observations.py to the table's
-- files as one block. Relation `silver`: the data files of silver.nbp_fx_rates (table × date × currency).
--   measure_key = nbp:nbp_fx_rates:PLN:<table>:<mid|bid|ask>@D: the table and the kind of rate define the
--                 measure (A and B publish a mid rate, C a bid and an ask); the currency is the breakdown
--   date_key    = the table's effective date. Table B is published once a week: its series are daily
--                 dated fixings with a value on the publication day only, never filled between
--   geography   = none (_Z): a rate belongs to no territory
--   value       = PLN per 1 unit of the currency, as the API publishes it; a rate the table does not
--                 publish (bid and ask in A and B, mid in C) is no row
-- Silver keeps one row per table × date × currency: where NBP published a corrected table for a date, the
-- one with the highest table number (silver.nbp_fx_rates, published_variants); nothing is chosen here.
with named as (
    select
        s.*,
        -- the currency's name in the newest table that lists it: one name per series, the same on every run
        arg_max(s.currency_name, s.effective_date) over (
            partition by s.table_code, s.currency_code
        ) as newest_currency_name
    from silver as s
),

rates as (
    select
        table_code,
        effective_date,
        currency_code,
        newest_currency_name as currency_name,
        source_version,
        raw_sha256,
        unnest(['mid', 'bid', 'ask']) as rate_type,
        unnest([mid_rate, bid_rate, ask_rate]) as rate
    from named
)

select
    'nbp' as source_system,
    'nbp_fx_rates' as block_id,
    'NBP exchange rates, tables A, B and C' as block_name,
    'nbp:nbp_fx_rates:PLN:' || table_code || ':' || rate_type || '@D' as measure_key,
    'Exchange rate, NBP table ' || table_code || ', ' || rate_type || ' (PLN per 1 unit)' as measure_name,
    table_code || ':' || rate_type as measure_codes,
    'PLN' as unit_code,
    'PLN per 1 unit of the currency' as unit_name,
    cast(null as varchar) as geography_source_code,
    '_Z' as geography_key,
    'D' as frequency,
    effective_date as date_key,
    cast(effective_date as varchar) as period_label,
    cast(to_json(map(['currency'], [currency_code])) as varchar) as breakdown_json,
    cast(to_json(map(['currency'], [currency_name])) as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    true as is_total,
    cast(rate as double) as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from rates
where rate is not null and effective_date is not null
