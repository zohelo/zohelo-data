{# URE electricity price indicators (register PL-ENV-020) from the newest Landing copy of each indicator page of
   www.ure.gov.pl (bronze.ure_table_cells, one row per cell of the page's indicator table; layouts checked in Bronze).
   Grain: indicator_code x frequency x period_label.
   - page_7852 "Średnia cena sprzedaży energii elektrycznej na rynku konkurencyjnym": columns 1 year, 2 yearly
     average, 3-6 quarterly averages Q1-Q4, 7 the URE notice numbers (Informacja/Komunikat Prezesa URE); zł/MWh.
   - page_7853 "Średnia cena energii elektrycznej dla gospodarstw domowych": columns 1 year, 2 yearly price including
     the distribution charge, 3 the notice number; zł/kWh.
   indicator_name is the value column's header as published (the definition URE gives), unit_text the unit in its
   brackets. Values as published (decimal comma read, no rescaling). Not loaded: '‒' (not published for that period:
   quarters before Q2 2015) and empty cells (not published yet); any other text keeps its row with value null and
   status 'unparsed', which fails the not_null test on value. #}
with newest as (
    select *
    from {{ source('ure', 'ure_table_cells') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

years as (
    select
        entity,
        row_number,
        cast(value_text as integer) as year
    from newest
    where column_number = 1 and regexp_full_match(value_text, '[0-9]{4}')
),

refs as (
    select
        n.entity,
        n.row_number,
        n.value_text as announcement_refs
    from newest as n
    where (n.entity = 'page_7852' and n.column_number = 7) or (n.entity = 'page_7853' and n.column_number = 3)
),

headers as (
    select
        entity,
        column_number,
        value_text as header_text
    from newest
    where row_number = 1
),

cells as (
    select
        n.entity,
        y.year,
        case n.entity when 'page_7852' then 'competitive_market_price' when 'page_7853' then 'household_price' end
            as indicator_code,
        case when n.column_number = 2 then 'A' else 'Q' end as frequency,
        case when n.column_number between 3 and 6 then n.column_number - 2 end as quarter,
        -- the value column's header: 7852 yearly = header cell 2, quarterly = header cell 3 (colspan 4)
        case when n.column_number = 2 then 2 else 3 end as header_column,
        trim(n.value_text) as value_text,
        n.page_title,
        n.page_modified,
        n.source_version,
        n.raw_sha256,
        n.ingested_at,
        n.row_number
    from newest as n
    inner join years as y on n.entity = y.entity and n.row_number = y.row_number
    where
        (n.entity = 'page_7852' and n.column_number between 2 and 6)
        or (n.entity = 'page_7853' and n.column_number = 2)
)

select
    c.entity,
    c.indicator_code,
    h.header_text as indicator_name,
    regexp_extract(h.header_text, '\[([^\]]+)\]', 1) as unit_text,
    case regexp_extract(h.header_text, '\[([^\]]+)\]', 1)
        when 'zł/MWh' then 'pln_per_mwh'
        when 'zł/kWh' then 'pln_per_kwh'
    end as unit_code,
    c.frequency,
    case when c.frequency = 'A' then cast(c.year as varchar) else c.year || '-Q' || c.quarter end as period_label,
    make_date(c.year, coalesce((c.quarter - 1) * 3 + 1, 1), 1) as period_start,
    c.value_text,
    case
        when regexp_full_match(c.value_text, '-?[0-9][0-9 ]*(,[0-9]+)?')
            then cast(replace(replace(c.value_text, ' ', ''), ',', '.') as double)
    end as value,
    case when regexp_full_match(c.value_text, '-?[0-9][0-9 ]*(,[0-9]+)?') then 'ok' else 'unparsed' end
        as observation_status,
    r.announcement_refs,
    c.page_title,
    cast(strptime(c.page_modified, '%d.%m.%Y') as date) as page_modified,
    c.source_version,
    c.raw_sha256,
    c.ingested_at
from cells as c
inner join headers as h on c.entity = h.entity and c.header_column = h.column_number
left join refs as r on c.entity = r.entity and c.row_number = r.row_number
where c.value_text not in ('', '‒', '–', '-', '—')
