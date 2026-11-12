{# KRUS pensions and benefits paid (sheet "Świadczenia" of the KRUS statistics workbook, newest copy): one row
   per period × benefit type × measure. Periods are quarters ('I kwartał 2020 r.', period_type Q) and years
   ('2020 rok', period_type A); a trailing footnote mark ('2)') is kept in footnote (from III quarter 2025 the
   survivors' pensions include the pensions paid together with them). benefit_key from the header rows (first
   header row: benefit group; second: 'Ogółem' / 'w tym wypadkowe'; both forward-filled to the right within the
   group); measure 'beneficiaries' (average monthly number of persons) or 'avg_benefit_pln' (average monthly
   benefit in zloty), from the last header row. #}
with cells as (
    select
        *,
        trim(regexp_replace(value_text, '\s+', ' ', 'g')) as text,
        try_cast(value_text as double) as number_value
    from ({{ xlsx_newest_cells(source('krus', 'krus_cells')) }}) as newest_cells
    where sheet_name = 'Świadczenia'
),

periods as (
    select
        row_number,
        text as period_label,
        case
            when regexp_matches(text, '^(I|II|III|IV) kwartał [0-9]{4}') then 'Q'
            when regexp_matches(text, '^[0-9]{4} rok') then 'A'
        end as period_type,
        cast(regexp_extract(text, '([0-9]{4})', 1) as integer) as year,
        list_position(['I', 'II', 'III', 'IV'], regexp_extract(text, '^(I|II|III|IV) kwartał', 1)) as quarter,
        nullif(regexp_extract(text, '([0-9])\)\s*$', 1), '') as footnote
    from cells
    where column_number = 1
),

first_period as (
    select min(row_number) as first_row
    from periods
    where period_type is not null
),

headers as (
    select
        c.row_number,
        c.column_number,
        c.text,
        dense_rank() over (order by c.row_number) as level,
        count(distinct c.row_number) over () as levels
    from cells as c
    inner join first_period as f on c.row_number < f.first_row
    where c.column_number >= 2 and c.text <> ''
),

value_columns as (
    select distinct c.column_number
    from cells as c
    inner join first_period as f on c.row_number >= f.first_row
    where c.column_number >= 2 and c.number_value is not null
),

groups as (
    select
        v.column_number,
        h.text as benefit_group,
        h.column_number as group_start
    from value_columns as v
    inner join headers as h on v.column_number >= h.column_number
    where h.level = 1
    qualify row_number() over (partition by v.column_number order by h.column_number desc) = 1
),

subgroups as (
    select
        g.column_number,
        h.text as subgroup
    from groups as g
    inner join headers as h on g.column_number >= h.column_number and g.group_start <= h.column_number
    where h.level = 2 and h.levels > 2
    qualify row_number() over (partition by g.column_number order by h.column_number desc) = 1
),

measures as (
    select
        v.column_number,
        h.text as measure_label
    from value_columns as v
    inner join headers as h on v.column_number = h.column_number
    where h.level = h.levels
),

keyed as (
    select
        g.column_number,
        g.benefit_group,
        s.subgroup,
        case
            when g.benefit_group = 'Emerytury i renty ogółem' then 'all'
            when g.benefit_group = 'Emerytury' then 'old_age'
            when g.benefit_group like 'Renty z tytułu niezdolności do pracy%' then 'disability'
            when g.benefit_group like 'Renty rodzinne%' then 'survivors'
            when g.benefit_group like 'GBRZ%' then 'gbrz'
            when g.benefit_group like 'Rodzicielskie świadczenia uzupełniające%' then 'parental_supplementary'
            when
                g.benefit_group like 'Świadczenia pieniężne dla cywilnych niewidomych%'
                then 'civilian_blind_war_victims'
        end || case when s.subgroup like 'w tym wypadkowe%' then '_accident' else '' end as benefit_key,
        case
            when m.measure_label like 'przeciętna miesięczna liczba%' then 'beneficiaries'
            when m.measure_label like 'przeciętne miesięczne świadczenie%' then 'avg_benefit_pln'
        end as measure
    from groups as g
    left join subgroups as s on g.column_number = s.column_number
    left join measures as m on g.column_number = m.column_number
)

select
    p.period_type,
    p.year,
    p.quarter,
    p.period_label,
    p.footnote,
    k.benefit_key,
    k.benefit_group,
    k.subgroup,
    k.measure,
    c.number_value as value,
    c.raw_sha256,
    c.source_version,
    c.ingested_at,
    case
        when p.period_type = 'Q' then make_date(p.year, 3 * p.quarter - 2, 1)
        else make_date(p.year, 1, 1)
    end as period_start
from periods as p
inner join cells as c on p.row_number = c.row_number
inner join keyed as k on c.column_number = k.column_number
where p.period_type is not null and c.number_value is not null
