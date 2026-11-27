{# KOWR weekly grain report (register PLS-033), table "Eksportowe ceny zbóż na rynkach zagranicznych (na bazie FOB)"
   (bronze.kowr_markets_cells, table_code 'fob'): export prices FOB in EUR/t of milling wheat, feed barley and maize
   at the US Gulf, Rouen, Bordeaux and the Black Sea ports, as of the quotation date the table names (one date per
   report, a weekday after the report week). Stated source: "opracowanie Biura Analiz i Strategii KOWR na podstawie
   danych KE, EBC, NBP" (kept in stated_source). Only the EUR price is kept: the changes in % and the zł/t price are
   KOWR's own derivations (NBP rate). "-" (brak danych) and a lone "0" (Bordeaux, 2023) keep value null, the text in
   value_flag; never 0. A row whose zł/t price disagrees with its EUR price times the NBP rate (Bronze cell
   'kontrola'; Black Sea barley in 2024-2025) keeps value null with value_flag 'zl_t_mismatch', as it cannot be told
   which of the two is wrong. A quotation date printed by two reports
   keeps the newer report's row (ranked by report week, so two files of one week and one key fail the unique test).
   Grain: commodity_code x market_code x quote_date. #}
with cells as (
    select
        entity,
        cast(strptime(substr(entity, 7), '%Y%m%d') as date) as report_week,
        cast(period_start as date) as quote_date,
        row_group,
        row_label,
        value_text,
        stated_source,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('kowr_markets', 'kowr_markets_cells')) }})
    where table_code = 'fob' and column_label = 'cena'
),

checks as (
    select
        entity as check_entity,
        row_group as check_group,
        row_label as check_label
    from ({{ xlsx_newest_cells(source('kowr_markets', 'kowr_markets_cells')) }})
    where table_code = 'fob' and column_label = 'kontrola'
)

select
    case s.row_group
        when 'Pszenica konsumpcyjna' then 'wheat'
        when 'Jęczmień paszowy' then 'feed_barley'
        when 'Kukurydza' then 'maize'
    end as commodity_code,
    s.row_group as commodity_name,
    case s.row_label
        when 'HRW (USA, Gulf)' then 'hrw_us_gulf'
        when 'SRW (USA, Gulf)' then 'srw_us_gulf'
        when 'USA (Gulf)' then 'us_gulf'
        when 'Francja (Rouen)' then 'rouen'
        when 'Francja (Bordeaux)' then 'bordeaux'
        when 'porty Morza Czarnego' then 'black_sea'
    end as market_code,
    s.row_label as market_name,
    case
        when s.row_label like '%USA%' then 'US'
        when s.row_label like 'Francja%' then 'FR'
        when s.row_label = 'porty Morza Czarnego' then 'BLACK_SEA'
    end as area_code,
    s.quote_date,
    s.report_week,
    'EUR_T' as unit_code,
    case
        when c.check_entity is null and s.value_text not in ('-', '0')
            then cast(replace(s.value_text, ',', '.') as decimal(12, 2))
    end as value,
    case
        when c.check_entity is not null then 'zl_t_mismatch'
        when s.value_text in ('-', '0') then s.value_text
    end as value_flag,
    s.stated_source,
    s.entity,
    s.source_version,
    s.raw_sha256
from cells as s
left join checks as c
    on s.entity = c.check_entity and s.row_group = c.check_group and s.row_label = c.check_label
qualify rank() over (partition by s.row_group, s.row_label, s.quote_date order by s.report_week desc) = 1
