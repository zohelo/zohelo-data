{# KOWR weekly grain report (register PLS-033), table "Ceny wybranych zbóż na krajowych giełdach towarowych"
   (bronze.kowr_markets_cells, table_code 'exchange'): sale offers on Polish commodity exchanges in zł/t, minimum,
   maximum and average, for the report week (Monday to Sunday). Stated source: "opracowanie Biura Analiz i Strategii
   KOWR na podstawie danych z krajowych giełd towarowych". The changes of the average in % are derivations and not
   kept. "-" (brak danych) keeps the price null; a row whose average is "-" keeps all three null. Weekly: Silver only,
   outside the curated Gold facts (no weekly fact model, as mrirw_zsrir's weekly tables).
   Grain: commodity_name x report_week. #}
with cells as (
    select
        entity,
        cast(strptime(substr(entity, 7), '%Y%m%d') as date) as report_week,
        period_text,
        row_label,
        column_label,
        try_cast(replace(value_text, ' ', '') as decimal(12, 2)) as price,
        stated_source,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('kowr_markets', 'kowr_markets_cells')) }})
    where table_code = 'exchange' and column_label in ('minimalna', 'maksymalna', 'średnia')
)

select
    row_label as commodity_name,
    report_week,
    period_text,
    'PLN_T' as unit_code,
    max(price) filter (where column_label = 'minimalna') as price_min,
    max(price) filter (where column_label = 'maksymalna') as price_max,
    max(price) filter (where column_label = 'średnia') as price_avg,
    count(*) as cells,
    stated_source,
    entity,
    source_version,
    raw_sha256
from cells
group by all
