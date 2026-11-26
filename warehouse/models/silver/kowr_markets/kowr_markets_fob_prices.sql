{# KOWR weekly grain report (register PLS-033), table "Eksportowe ceny zbóż na rynkach zagranicznych (na bazie FOB)"
   (bronze.kowr_markets_cells, table_code 'fob'): export prices FOB in EUR/t of milling wheat, feed barley and maize
   at the US Gulf, Rouen, Bordeaux and the Black Sea ports, as of the quotation date the table names (one date per
   report, a weekday after the report week). Stated source: "opracowanie Biura Analiz i Strategii KOWR na podstawie
   danych KE, EBC, NBP" (kept in stated_source). Only the EUR price is kept: the changes in % and the zł/t price are
   KOWR's own derivations (NBP rate). "-" (brak danych) keeps value null. A quotation date printed by two reports
   keeps the newer report's row (ranked by report week, so two files of one week and one key fail the unique test).
   Grain: commodity_code x market_code x quote_date. #}
with cells as (
    select
        entity,
        cast(strptime(substr(entity, 7), '%Y%m%d') as date) as report_week,
        cast(strptime(period_text, '%d.%m.%Y') as date) as quote_date,
        row_group,
        row_label,
        value_text,
        stated_source,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('kowr_markets', 'kowr_markets_cells')) }})
    where table_code = 'fob' and column_label = 'cena'
)

select
    case row_group
        when 'Pszenica konsumpcyjna' then 'wheat'
        when 'Jęczmień paszowy' then 'feed_barley'
        when 'Kukurydza' then 'maize'
    end as commodity_code,
    row_group as commodity_name,
    case row_label
        when 'HRW (USA, Gulf)' then 'hrw_us_gulf'
        when 'SRW (USA, Gulf)' then 'srw_us_gulf'
        when 'USA (Gulf)' then 'us_gulf'
        when 'Francja (Rouen)' then 'rouen'
        when 'Francja (Bordeaux)' then 'bordeaux'
        when 'porty Morza Czarnego' then 'black_sea'
    end as market_code,
    row_label as market_name,
    case
        when row_label like '%USA%' then 'US'
        when row_label like 'Francja%' then 'FR'
        when row_label = 'porty Morza Czarnego' then 'BLACK_SEA'
    end as area_code,
    quote_date,
    report_week,
    'EUR_T' as unit_code,
    try_cast(replace(value_text, ',', '.') as decimal(12, 2)) as value,
    value_text,
    stated_source,
    entity,
    source_version,
    raw_sha256
from cells
qualify rank() over (partition by row_group, row_label, quote_date order by report_week desc) = 1
