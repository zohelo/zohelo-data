{# URE energy price indicators (register PL-ENV-020, #2870): one row per indicator x period of the three ure.gov.pl
   indicator pages (bronze.ure_prices_cells, newest copy of each page). Grain: indicator_code x period_label.
   Indicators, read from the column header text (the Bronze loader accepts only the known headers):
   - electricity_competitive_market (page 7852): the yearly average price ('Średnia roczna cena ...', frequency A,
     from 2008) and the quarterly average price ('Średnia kwartalna cena ... | I kwartał' ..., frequency Q, from
     2015 Q2), PLN/MWh. A year and its quarters are separate periods ('2025' and '2025-Q1').
   - electricity_households (page 7853): the yearly average price for a household customer including the
     distribution charge, excise included, VAT excluded, PLN/kWh, from 2012.
   - gas_import_eu_efta (page 7859, table 1: 'Średnie ceny zakupu gazu ziemnego z zagranicy ...', the page title
     names imports from EU or EFTA member states) and gas_import_non_eu_efta (table 2), quarterly, PLN/MWh; 2013
     and 2014 are printed in the cell with their own unit ('1,31 zł/m3'), kept as PLN/m3.
   Cells: a number with decimal comma is value_status 'ok'; an empty cell (not published yet) is 'empty' and the
   figure dash '‒' (no such price for the period) is 'dash', both with value null, never 0; any other text is
   'unparsed' (value null), which fails accepted_values. The 'Nr Informacji' column (the URE notices of the row) is
   notice_refs. #}
with versions as (
    select
        entity,
        max(ingested_at) as ingested_at
    from {{ source('ure_prices', 'ure_prices_cells') }}
    group by entity
),

newest as (
    select c.*
    from {{ source('ure_prices', 'ure_prices_cells') }} as c
    inner join versions as v on c.entity = v.entity and c.ingested_at = v.ingested_at
),

notices as (
    select
        entity,
        table_number,
        row_label,
        cell_text as notice_refs
    from newest
    where starts_with(column_header, 'Nr Informacji')
),

cells as (
    select
        c.*,
        n.notice_refs,
        split_part(c.column_header, ' | ', 1) as indicator_label,
        nullif(split_part(c.column_header, ' | ', 2), '') as quarter_label,
        regexp_extract(c.cell_text, '^(-?[0-9][0-9 ]*(,[0-9]+)?)( (zł/m3))?$', 1) as number_text,
        regexp_extract(c.cell_text, '^(-?[0-9][0-9 ]*(,[0-9]+)?)( (zł/m3))?$', 4) as cell_unit
    from newest as c
    left join notices as n on c.entity = n.entity and c.table_number = n.table_number and c.row_label = n.row_label
    where not starts_with(c.column_header, 'Nr Informacji')
),

typed as (
    select
        *,
        case
            when
                starts_with(
                    indicator_label, 'Średnia roczna cena sprzedaży energii elektrycznej na rynku konkurencyjnym'
                )
                or starts_with(
                    indicator_label, 'Średnia kwartalna cena sprzedaży energii elektrycznej na rynku konkurencyjnym'
                )
                then 'electricity_competitive_market'
            when starts_with(indicator_label, 'Cena energii elektrycznej dla odbiorcy w gospodarstwie domowym')
                then 'electricity_households'
            when starts_with(indicator_label, 'Średnie ceny zakupu gazu ziemnego z zagranicy') then 'gas_import_eu_efta'
            when starts_with(indicator_label, 'Średnia cena zakupu gazu ziemnego sprowadzanego z innych państw')
                then 'gas_import_non_eu_efta'
        end as indicator_code,
        case quarter_label
            when 'I kwartał' then 1 when 'II kwartał' then 2 when 'III kwartał' then 3 when 'IV kwartał' then 4
        end as period_quarter,
        coalesce(nullif(cell_unit, ''), regexp_extract(indicator_label, '\[(zł/[A-Za-z0-9]+)\]', 1)) as unit_published,
        case
            when number_text <> '' then 'ok'
            when cell_text = '' then 'empty'
            when cell_text = '‒' then 'dash'
            else 'unparsed'
        end as value_status
    from cells
)

select
    entity,
    indicator_code,
    indicator_label,
    case when quarter_label is null then 'A' else 'Q' end as frequency,
    cast(row_label as integer) as period_year,
    period_quarter,
    row_label || coalesce('-Q' || period_quarter, '') as period_label,
    make_date(cast(row_label as integer), coalesce(period_quarter * 3 - 2, 1), 1) as period_start,
    case unit_published
        when 'zł/MWh' then 'pln_per_mwh'
        when 'zł/kWh' then 'pln_per_kwh'
        when 'zł/m3' then 'pln_per_m3'
    end as unit_code,
    unit_published,
    case
        when value_status = 'ok' then cast(replace(replace(number_text, ' ', ''), ',', '.') as decimal(18, 4))
    end as value,
    value_status,
    cell_text,
    notice_refs,
    cast(strptime(page_modified, '%d.%m.%Y') as date) as page_modified,
    cast(strptime(page_published, '%d.%m.%Y') as date) as page_published,
    table_number,
    column_number,
    source_version,
    raw_sha256,
    ingested_at
from typed
