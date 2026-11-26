{# KOWR weekly grain report (register PLS-033), table "Ceny zakupu zbóż w krajach UE" (bronze.kowr_markets_cells,
   table_code 'eu'): purchase prices of wheat, barley and maize in EUR/t in the 27 member states and the EU average
   ("Średnio w UE") for the report week (Monday to Sunday). Stated source: "opracowanie Biura Analiz i Strategii KOWR
   na podstawie danych Komisji Europejskiej" (kept in stated_source). Only the EUR price is kept: the changes in % and
   the PLN price (ECB rate) are derivations. Country names as printed. "-" (brak danych) and "0" (printed where a
   member state reported nothing, e.g. Szwecja, week 23-29.06.2025) keep value null, the text in value_flag; never 0.
   Weekly: Silver only, outside the curated Gold facts (no weekly fact model, as mrirw_zsrir's weekly tables).
   Grain: commodity_code x country_name x report_week. #}
with cells as (
    select
        entity,
        cast(strptime(substr(entity, 7), '%Y%m%d') as date) as report_week,
        period_text,
        row_group,
        row_label,
        value_text,
        stated_source,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('kowr_markets', 'kowr_markets_cells')) }})
    where table_code = 'eu' and column_label = 'Cena w EUR/t'
)

select
    case row_group
        when 'Pszenica' then 'wheat'
        when 'Jęczmień' then 'barley'
        when 'Kukurydza' then 'maize'
    end as commodity_code,
    row_group as commodity_name,
    row_label as country_name,
    report_week,
    period_text,
    'EUR_T' as unit_code,
    case when value_text not in ('-', '0') then cast(replace(value_text, ',', '.') as decimal(12, 2)) end as value,
    case when value_text in ('-', '0') then value_text end as value_flag,
    stated_source,
    entity,
    source_version,
    raw_sha256
from cells
