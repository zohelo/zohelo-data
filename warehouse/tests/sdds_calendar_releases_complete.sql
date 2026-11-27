-- sdds_calendar (TP-012, TP-013): every calendar workbook in Silver is one copy of one calendar year and lists releases
-- of both entries built from it, NBP and the Ministry of Finance, and of each of their dated categories of the 2026
-- workbook; Silver is not empty. A layout change that loses a table or a category fails here instead of thinning the
-- counts silently (a category the producer really drops is removed from the list below by a person). One test per
-- Silver model (it reads no other model).
with expected as (
    select *
    from (
        values
        ('nbp', 'Zestawienie wybranych pozycji z bilansu monetarnych instytucji finansowych'),
        ('nbp', 'Wybrane dane z bilansu NBP'),
        ('nbp', 'Bilans Płatniczy'),
        ('nbp', 'Rezerwy oficjalne brutto'),
        ('nbp', 'Płynne aktywa i pasywa w walutach obcych'),
        ('nbp', 'Międzynarodowa pozycja inwestycyjna'),
        ('nbp', 'Zadłużenie zagraniczne'),
        ('mf', 'Operacje szczebla centralnego sektora instytucji rządowych i samorządowych'),
        ('mf', 'Dług krajowy Skarbu Państwa'),
        ('mf', 'Dług zagraniczny Skarbu Państwa'),
        ('mf', 'Państwowy dług publiczny')
    ) as t (publisher, category)
),

copies as (
    select
        entity,
        count(distinct raw_sha256) as copies,
        count(distinct calendar_year) as years,
        max(calendar_year) as calendar_year
    from {{ ref('sdds_calendar_releases') }}
    group by entity
),

listed as (
    select distinct
        entity,
        publisher,
        category
    from {{ ref('sdds_calendar_releases') }}
)

select
    entity as item,
    copies || ' copies, ' || years || ' calendar years' as problem
from copies
where copies <> 1 or years <> 1 or entity <> 'calendar_' || calendar_year
union all
select
    c.entity || ' ' || e.category as item,
    'no release listed for this ' || e.publisher || ' category' as problem
from copies as c
cross join expected as e
left join listed as l on c.entity = l.entity and e.publisher = l.publisher and e.category = l.category
where l.category is null
union all
select
    'all' as item,
    'no releases' as problem
from (select count(*) as n from {{ ref('sdds_calendar_releases') }}) as t
where t.n = 0
