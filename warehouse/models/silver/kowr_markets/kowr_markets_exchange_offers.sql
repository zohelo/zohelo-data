{# KOWR weekly grain report (register PLS-033), table "Ceny wybranych zbóż na krajowych giełdach towarowych"
   (bronze.kowr_markets_cells, table_code 'exchange'): sale offers on Polish commodity exchanges in zł/t, minimum,
   maximum and average, for the period the table names (Monday to Sunday, sometimes the week before the report's).
   Stated source: "opracowanie Biura Analiz i Strategii KOWR na podstawie danych z krajowych giełd towarowych" (null
   where a report prints no source line under the table, two reports of January 2026). The changes of the average in %
   are derivations and not kept. "-" (brak danych) keeps a price null. A row whose average lies outside its minimum
   and maximum (Bronze cell 'kontrola') keeps all three null with value_flag 'avg_outside_range'. A period printed by
   two reports keeps the newer report's row. Weekly: Silver only, outside the curated Gold facts (no weekly fact
   model, as mrirw_zsrir's weekly tables). Grain: commodity_name x period_start. #}
with cells as (
    select
        entity,
        cast(strptime(substr(entity, 7), '%Y%m%d') as date) as report_week,
        period_text,
        cast(period_start as date) as period_start,
        cast(period_end as date) as period_end,
        row_label,
        column_label,
        try_cast(replace(value_text, ' ', '') as decimal(12, 2)) as price,
        nullif(stated_source, '') as stated_source,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('kowr_markets', 'kowr_markets_cells')) }})
    where table_code = 'exchange' and column_label in ('minimalna', 'maksymalna', 'średnia')
),

checks as (
    select
        entity as check_entity,
        row_label as check_label
    from ({{ xlsx_newest_cells(source('kowr_markets', 'kowr_markets_cells')) }})
    where table_code = 'exchange' and column_label = 'kontrola'
),

offers as (
    select
        row_label as commodity_name,
        period_start,
        period_end,
        period_text,
        report_week,
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
)

select
    o.commodity_name,
    o.period_start,
    o.period_end,
    o.period_text,
    o.report_week,
    'PLN_T' as unit_code,
    case when c.check_entity is null then o.price_min end as price_min,
    case when c.check_entity is null then o.price_max end as price_max,
    case when c.check_entity is null then o.price_avg end as price_avg,
    case when c.check_entity is not null then 'avg_outside_range' end as value_flag,
    o.cells,
    o.stated_source,
    o.entity,
    o.source_version,
    o.raw_sha256
from offers as o
left join checks as c on o.entity = c.check_entity and o.commodity_name = c.check_label
qualify rank() over (partition by o.commodity_name, o.period_start order by o.report_week desc) = 1
