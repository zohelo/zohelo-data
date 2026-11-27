{# KOWR weekly grain report (register PLS-033), table "Ceny zakupu zbóż w krajach UE" (bronze.kowr_markets_cells,
   table_code 'eu'): purchase prices of wheat, barley and maize in EUR/t in the 27 member states and the EU average
   ("Średnio w UE") for the period the table names (Monday to Sunday, often the week before the report's, once two
   weeks: 18-31.12.2023). Stated source: "opracowanie Biura Analiz i Strategii KOWR na podstawie danych Komisji
   Europejskiej" (kept in stated_source). Only the EUR price is kept: the changes in % and the PLN price (ECB rate) are
   derivations. Country names as printed. "-" (brak danych) and "0" (printed where a member state reported nothing,
   e.g. Szwecja, 23-29.06.2025) keep value null, the text in value_flag; never 0. A cell whose PLN price disagrees with
   its EUR price times the ECB rate (Bronze cell 'kontrola'; at most two per block, more reject the file; Polska is
   not checked, its PLN price is ZSRIR's) keeps value null with value_flag 'pln_mismatch'. A period printed by two
   reports keeps the newer report's row (ranked by report week, so two files of one week fail the unique test).
   Weekly: Silver only, outside the curated Gold facts (no weekly fact model, as mrirw_zsrir's weekly tables).
   Grain: commodity_code x country_name x period_start. #}
with cells as (
    select
        entity,
        cast(strptime(substr(entity, 7), '%Y%m%d') as date) as report_week,
        period_text,
        cast(period_start as date) as period_start,
        cast(period_end as date) as period_end,
        row_group,
        row_label,
        value_text,
        stated_source,
        source_version,
        raw_sha256
    from ({{ xlsx_newest_cells(source('kowr_markets', 'kowr_markets_cells')) }})
    where table_code = 'eu' and column_label = 'Cena w EUR/t'
),

checks as (
    select
        entity as check_entity,
        row_group as check_group,
        row_label as check_label
    from ({{ xlsx_newest_cells(source('kowr_markets', 'kowr_markets_cells')) }})
    where table_code = 'eu' and column_label = 'kontrola'
)

select
    case s.row_group
        when 'Pszenica' then 'wheat'
        when 'Jęczmień' then 'barley'
        when 'Kukurydza' then 'maize'
    end as commodity_code,
    s.row_group as commodity_name,
    s.row_label as country_name,
    s.period_start,
    s.period_end,
    s.period_text,
    s.report_week,
    'EUR_T' as unit_code,
    case
        when c.check_entity is null and s.value_text not in ('-', '0')
            then cast(replace(s.value_text, ',', '.') as decimal(12, 2))
    end as value,
    case
        when c.check_entity is not null then 'pln_mismatch'
        when s.value_text in ('-', '0') then s.value_text
    end as value_flag,
    s.stated_source,
    s.entity,
    s.source_version,
    s.raw_sha256
from cells as s
left join checks as c
    on s.entity = c.check_entity and s.row_group = c.check_group and s.row_label = c.check_label
qualify rank() over (partition by s.row_group, s.row_label, s.period_start order by s.report_week desc) = 1
