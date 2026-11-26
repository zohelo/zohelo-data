{# ZUS Rocznik Statystyczny Ubezpieczeń Społecznych (register PLS-010): curated tables of each edition, read from the
   PDF text layer (bronze.zus_yearbook_lines, newest copy of each edition). Grain: measure_code x area_name x
   period_year (an edition covers three years; editions do not overlap).
   Tables, found by title, never by page number:
     payers_voiv   "Tabl. 8. Płatnicy według województw. Stan w dniu 31 grudnia": active contribution payers (accounts)
     insured_voiv  "Tabl. 9. Ubezpieczeni według województw. Stan w dniu 31 grudnia": persons insured in old-age and
                   disability pension insurance; area_basis from the table's footnote: the payer's seat (editions
                   2009-2011) or the insured person's registered residence (from 2012-2014)
   From 2012 the voivodeships add up to 97-100 % of Poland (persons and payers the table assigns to no voivodeship).
   Layout: the table occupies one half of a page (the 2021-2023 edition prints two pages on one PDF page, so a line
   holds both halves); its half runs from the title's column to the next title further right on the page, its lines
   from the title to the next title of another table below it (the English title repeats the number). The header
   line gives the three years; every area row is the area name (Polska and the 16 voivodeships, as printed, footnote
   letters dropped) followed by exactly one value per year. Values are split on runs of two or more spaces; a value
   is digits in groups of three separated by one space, digits without spaces (2015-2017 edition), or a ZUS sign
   ('-' or '–' nothing, '.' no data, 'x' not applicable): signs are kept as cell_status with no value, never 0
   (none occur in 2009-2023). A row whose value count differs from the year count yields nothing
   (tests/zus_yearbook_tables_complete.sql then fails). #}
{% set areas = ['polska', 'dolnośląskie', 'kujawsko-pomorskie', 'lubelskie', 'lubuskie', 'łódzkie', 'małopolskie',
    'mazowieckie', 'opolskie', 'podkarpackie', 'podlaskie', 'pomorskie', 'śląskie', 'świętokrzyskie',
    'warmińsko-mazurskie', 'wielkopolskie', 'zachodniopomorskie'] %}
with lines as (
    select
        entity as edition,
        page_number,
        line_number,
        -- 'Tabl.' (any case) becomes a five-character marker, so a split keeps every column in place; the narrow and
        -- fixed-width spaces of the newer editions (thousands separator U+2005, footnote gap U+2002) become one space
        regexp_replace(
            regexp_replace(line_text, '[\x{00A0}\x{2000}-\x{200A}\x{202F}]', ' ', 'g'),
            '[Tt][Aa][Bb][Ll]\.', '§§§§§', 'g'
        ) as line_text,
        raw_sha256,
        source_version
    from {{ source('zus_yearbook', 'zus_yearbook_lines') }}
    qualify dense_rank() over (partition by entity order by source_version desc) = 1
),

-- every 'Tabl.' on a line, with its column (1-based)
marks as (
    select
        l.edition,
        l.page_number,
        l.line_number,
        length(array_to_string(list_slice(string_split(l.line_text, '§§§§§'), 1, s.k), '§§§§§')) + 1 as col,
        string_split(l.line_text, '§§§§§')[s.k + 1] as after_mark,
        -- the table number; the English title repeats it under the Polish one (2021-2023 layout)
        nullif(regexp_extract(string_split(l.line_text, '§§§§§')[s.k + 1], '^\s*([0-9]+)', 1), '') as table_number
    from lines as l,
        generate_series(1, len(string_split(l.line_text, '§§§§§')) - 1) as s (k)
),

titles as (
    select
        edition,
        page_number,
        line_number,
        col,
        table_number,
        case
            when regexp_matches(after_mark, '^\s*[0-9]+\.?\s+(?i:płatnicy\s+według\s+województw)') then 'payers'
            when regexp_matches(after_mark, '^\s*[0-9]+\.?\s+(?i:ubezpieczeni\s+według\s+województw)') then 'insured'
        end as measure_code
    from marks
),

-- the table's half and its lines: up to the next mark further right on the page, and the next mark of another table
-- in the same half below the title
bands as (
    select
        t.edition,
        t.page_number,
        t.line_number as title_line,
        t.measure_code,
        greatest(t.col - 12, 1) as col_from,
        coalesce((
            select min(m.col) from marks as m
            where m.edition = t.edition and m.page_number = t.page_number and m.col > t.col + 40
        ), 100000) as col_to,
        coalesce((
            select min(m.line_number) from marks as m
            where
                m.edition = t.edition and m.page_number = t.page_number and m.line_number > t.line_number
                and abs(m.col - t.col) <= 20 and m.table_number is distinct from t.table_number
        ), 100000) as line_to
    from titles as t
    where t.measure_code is not null
),

region as (
    select
        b.edition,
        b.measure_code,
        b.page_number,
        l.line_number,
        rtrim(substr(l.line_text, b.col_from, b.col_to - b.col_from)) as text,
        l.raw_sha256,
        l.source_version
    from bands as b
    inner join lines as l
        on
            b.edition = l.edition and b.page_number = l.page_number
            and b.title_line < l.line_number and b.line_to > l.line_number
),

-- the first line of the region with three years is the header
header as (
    select
        edition,
        measure_code,
        page_number,
        min_by(text, line_number) as text
    from region
    where regexp_matches(text, '(19|20)[0-9]{2}\s+(19|20)[0-9]{2}\s+(19|20)[0-9]{2}')
    group by all
),

years as (
    select
        edition,
        measure_code,
        page_number,
        list_transform(
            regexp_extract_all(text, '(?:19|20)[0-9]{2}'), lambda y: cast(y as integer)  -- noqa: RF02, RF03
        ) as years
    from header
),

parsed as (
    select
        r.*,
        lower(regexp_extract(trim(r.text), '^([A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż-]+)', 1)) as word,
        -- what follows the name and its dot leaders, cut on runs of two or more spaces
        list_filter(
            list_transform(
                regexp_split_to_array(
                    regexp_replace(trim(r.text), '^[A-Za-zĄĆĘŁŃÓŚŹŻąćęłńóśźż-]+[a-c]?(\s*\.)*', ''), '\s{2,}'
                ),
                lambda x: trim(x)  -- noqa: RF02, RF03
            ),
            lambda x: x <> ''  -- noqa: RF02, RF03
        ) as tokens
    from region as r
),

area_rows as (
    select
        p.*,
        (
            select a.area from (select unnest({{ areas }}) as area) as a
            where p.word = a.area or (p.word like a.area || '_' and length(p.word) = length(a.area) + 1)
        ) as area_name
    from parsed as p
),

-- the line after each area row (the Polska row is followed by the English "POLAND")
with_next as (
    select
        a.*,
        (
            select min_by(n.tokens, n.line_number) from parsed as n
            where
                n.edition = a.edition and n.measure_code = a.measure_code and n.page_number = a.page_number
                and n.line_number between a.line_number + 1 and a.line_number + 2 and len(n.tokens) > 0
                and n.word not in {{ areas | replace('[', '(') | replace(']', ')') }}
        ) as next_tokens
    from area_rows as a
    where a.area_name = 'polska'
),

polska_next as (
    select
        *,
        len(next_tokens) = 1 and regexp_full_match(next_tokens[1], '[0-9]{1,3}')
        and len(tokens) > 0 and regexp_full_match(tokens[1], '[0-9]{4,}') as joined
    from with_next
),

-- The Polska row prints its values either on its own line or on the "POLAND" line below it. The 2015-2017 edition's
-- text layer prints them on the Polska line without digit-group spaces and moves the leading digit group of the
-- first year alone to the "POLAND" line ("392685 ..." / "2" for 2 392 685): that group is put back only when the
-- "POLAND" line holds exactly one group of 1-3 digits and the first value has 4 or more digits without a space;
-- tests/zus_yearbook_tables_complete.sql bounds Poland by the sum of the voivodeships.
rows_with_values as (
    select
        a.edition,
        a.measure_code,
        a.page_number,
        a.line_number,
        a.area_name,
        case
            when len(a.tokens) > 0 and coalesce(w.joined, false)
                then list_concat([w.next_tokens[1] || a.tokens[1]], list_slice(a.tokens, 2, len(a.tokens)))
            when len(a.tokens) > 0 then a.tokens
            else w.next_tokens
        end as tokens,
        coalesce(w.joined, false) as value_joined,
        a.raw_sha256,
        a.source_version
    from area_rows as a
    left join polska_next as w
        on
            a.edition = w.edition and a.measure_code = w.measure_code and a.page_number = w.page_number
            and a.line_number = w.line_number
    where a.area_name is not null
),

-- how the table assigns persons to voivodeships, from its footnote: the payer's seat (2009-2011 editions) or the
-- insured person's registered residence (from 2012-2014); the payers table states no rule
basis as (
    select
        edition,
        measure_code,
        page_number,
        case
            when measure_code = 'payers' then 'not_stated'
            when bool_or(regexp_matches(lower(text), 'siedzib\w*\s+płatnik')) then 'payer_seat'
            when bool_or(regexp_matches(lower(text), 'miejsca\s+zameldowania')) then 'residence'
        end as area_basis
    from region
    group by edition, measure_code, page_number
),

cells as (
    select
        r.edition,
        r.measure_code,
        r.page_number,
        r.line_number,
        r.area_name,
        y.years[s.k] as period_year,
        r.tokens[s.k] as value_text,
        r.value_joined and s.k = 1 as value_joined,
        b.area_basis,
        r.raw_sha256,
        r.source_version
    from rows_with_values as r
    inner join years as y
        on r.edition = y.edition and r.measure_code = y.measure_code and r.page_number = y.page_number
    inner join basis as b
        on r.edition = b.edition and r.measure_code = b.measure_code and r.page_number = b.page_number,
        generate_series(1, len(y.years)) as s (k)
    where
        len(r.tokens) = len(y.years)
        and list_bool_and(
            list_transform(
                r.tokens, lambda x: regexp_matches(x, '^([0-9]{1,3}( [0-9]{3})*|[0-9]+|[-–.x])$')  -- noqa: RF02
            )
        )
)

select
    'zus_yearbook' as source_system,
    edition,
    case measure_code when 'payers' then 'payers_voiv' else 'insured_voiv' end as table_code,
    measure_code,
    case measure_code when 'payers' then 'payers' else 'persons' end as unit_code,
    area_name,
    area_basis,
    period_year,
    value_text,
    case
        when value_text in ('-', '–') then null
        when value_text in ('.', 'x') then null
        else cast(replace(value_text, ' ', '') as double)
    end as value,
    case
        when value_text in ('-', '–') then 'nil'
        when value_text = '.' then 'missing'
        when value_text = 'x' then 'not_applicable'
        else 'ok'
    end as cell_status,
    value_joined,
    page_number,
    line_number,
    source_version,
    raw_sha256
from cells
