{# MRiRW ZSRIR market bulletins (zsrir.minrol.gov.pl, register PLS-032): monthly average purchase prices from the
   monthly sheet of the newest XLSX issue of three weekly bulletins (bronze.mrirw_zsrir_cells), Poland.
   Grain: one row per market x product_code x period_start (month).
     cereals  sheet 'ZAKUP WYKRESY MCE': rows = months (Excel dates), columns = grains (consumption, feed, malting)
     milk     sheet 'MIESIĘCZNE CENY SKUPU 2004-<year>': rows = years, columns = months I-XII, raw cow's milk
     pork     sheet 'ZAKUP ŻYWIEC WYKRES MSC': rows = months I-XII, columns = years, pigs for slaughter, live weight
   The grid starts at the row whose column B holds the header text (the same layouts as SHEETS in
   ingestion/to_bronze/mrirw_zsrir/load.py, which rejects a workbook without them). The unit is read from the sheet's
   title, "(zł/...)", above the grid. Values as published; a cell that is not a number (ZSRIR prints '---', '--', '-'
   or 'nld', too few reports to publish a price) keeps value null and its text in value_flag, so it never becomes 0.
   A row or column label that does not parse leaves its key null, and the tests fail. #}
with cells as (
    select
        entity as market,
        trim(sheet_name) as sheet_name,
        row_number,
        column_number,
        cell_type,
        value_text,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text,
        raw_sha256,
        source_version,
        ingested_at
    from ({{ xlsx_newest_cells(source('mrirw_zsrir', 'mrirw_zsrir_cells')) }}) as newest_cells
),

layouts as (
    select * from (
        values
        ('cereals', '^ZAKUP WYKRESY MCE$', 'MC/ROK'),
        ('milk', '^MIESIĘCZNE CENY SKUPU 2004-20[0-9]{2}$', 'ROK'),
        ('pork', '^ZAKUP ŻYWIEC WYKRES MSC$', 'MIESIĄC')
    ) as t (market, sheet_pattern, header_text)
),

sheet as (
    select
        c.*,
        l.header_text
    from cells as c
    inner join layouts as l on c.market = l.market and regexp_matches(c.sheet_name, l.sheet_pattern)
),

-- exactly one header cell per sheet: a second grid header would make the rows below it ambiguous, so the market
-- then yields no rows and the completeness tests fail (no first-match pick)
header_rows as (
    select
        market,
        min(row_number) as header_row
    from sheet
    where column_number = 2 and text = header_text
    group by market
    having count(*) = 1
),

titles as (
    select
        s.market,
        string_agg(s.text, ' ' order by s.row_number, s.column_number) as title,
        max(case when s.text like 'Notowania%' then s.text end) as issue_label
    from sheet as s
    inner join header_rows as h on s.market = h.market and s.row_number < h.header_row
    group by s.market
),

roman as (
    select * from (
        values ('I', 1), ('II', 2), ('III', 3), ('IV', 4), ('V', 5), ('VI', 6), ('VII', 7), ('VIII', 8), ('IX', 9),
        ('X', 10), ('XI', 11), ('XII', 12)
    ) as t (numeral, month_number)
),

products as (
    select * from (
        values
        ('cereals', 'pszenica konsumpcyjna', 'wheat_consumption'),
        ('cereals', 'pszenica paszowa', 'wheat_feed'),
        ('cereals', 'żyto konsumpcyjne', 'rye_consumption'),
        ('cereals', 'żyto paszowe', 'rye_feed'),
        ('cereals', 'jęczmień konsumpcyjny', 'barley_consumption'),
        ('cereals', 'jęczmień paszowy', 'barley_feed'),
        ('cereals', 'jęczmień browarny', 'barley_malting'),
        ('cereals', 'kukurydza mokra', 'maize_wet'),
        ('cereals', 'kukurydza sucha', 'maize_dry'),
        ('cereals', 'owies konsumpcyjny', 'oats_consumption'),
        ('cereals', 'owies paszowy', 'oats_feed'),
        ('cereals', 'pszenżyto paszowe', 'triticale_feed')
    ) as t (market, product_label, product_code)
),

-- column headers of the grid (row = header row, column C onwards) and row labels (column B below it)
col_heads as (
    select
        s.market,
        s.column_number,
        s.text
    from sheet as s
    inner join header_rows as h on s.market = h.market and s.row_number = h.header_row
    where s.column_number > 2
),

row_heads as (
    select
        s.market,
        s.row_number,
        s.text,
        s.cell_type,
        try_cast(s.value_text as double) as number
    from sheet as s
    inner join header_rows as h on s.market = h.market and s.row_number > h.header_row
    where s.column_number = 2
),

grid as (
    select
        s.market,
        s.sheet_name,
        s.row_number,
        s.column_number,
        r.text as row_label,
        r.cell_type as row_label_type,
        r.number as row_number_value,
        ch.text as column_label,
        s.cell_type,
        s.value_text,
        s.text,
        s.raw_sha256,
        s.source_version,
        s.ingested_at
    from sheet as s
    inner join row_heads as r on s.market = r.market and s.row_number = r.row_number
    inner join col_heads as ch on s.market = ch.market and s.column_number = ch.column_number
),

parsed as (
    select
        g.*,
        case
            when g.market = 'cereals' and g.row_number_value = floor(g.row_number_value)
                then date '1899-12-30' + cast(g.row_number_value as integer)
            when g.market = 'milk'
                then make_date(try_cast(g.row_label as integer), rc.month_number, 1)
            when g.market = 'pork'
                then make_date(try_cast(g.column_label as integer), rr.month_number, 1)
        end as period_date,
        case g.market
            when 'cereals' then p.product_code
            when 'milk' then 'raw_milk'
            when 'pork' then 'live_pigs'
        end as product_code,
        case g.market
            when 'cereals' then g.column_label
            when 'milk' then 'Mleko surowe'
            when 'pork' then 'Tuczniki, żywiec'
        end as product_name
    from grid as g
    left join roman as rc on g.column_label = rc.numeral
    left join roman as rr on g.row_label = rr.numeral
    left join products as p on g.market = p.market and lower(g.column_label) = p.product_label
    -- rows below the grid (notes, sources) have a label that is no period: they are not part of the grid
    where
        (g.market = 'cereals' and g.row_label_type = 'n')
        or (g.market = 'milk' and regexp_matches(g.row_label, '^(19|20)[0-9]{2}$'))
        or (g.market = 'pork' and rr.numeral is not null)
),

units as (
    select
        market,
        issue_label,
        regexp_extract(title, '\((zł/[^)]*)\)', 1) as unit_text
    from titles
)

select
    p.market,
    p.product_code,
    p.product_name,
    -- a cereals date that is not the first of a month is no month: null fails not_null
    case when day(p.period_date) = 1 then cast(p.period_date as date) end as period_start,
    'M' as frequency,
    'purchase' as price_type,
    u.unit_text,
    case
        when u.unit_text = 'zł/t' then 'PLN_T'
        when u.unit_text in ('zł/kg w.ż.', 'zł/kg wagi żywej') then 'PLN_KG_LW'
        when u.unit_text = 'zł/100 kg' then 'PLN_100KG'
    end as unit_code,
    'PLN' as currency,
    case when p.cell_type = 'n' then try_cast(p.value_text as double) end as value,
    case when p.cell_type != 'n' then p.text end as value_flag,
    p.sheet_name,
    p.row_number as source_row,
    p.column_number as source_column,
    u.issue_label,
    p.raw_sha256,
    p.source_version,
    p.ingested_at
from parsed as p
left join units as u on p.market = u.market
