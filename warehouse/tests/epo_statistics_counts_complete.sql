-- EPO patent statistics (silver.epo_statistics_counts): proves each edition's workbooks were read as published, from
-- the model alone. Returns edition x table x year x problem for: a table of the selection missing from an edition; a
-- year without its 'Total' row (country and region tables) or without the 35 named fields (field tables); countries
-- (with 'Not classified') or regions that do not add up to their 'Total'; fields that do not add up to the edition's
-- total of the same indicator and year; field x country rows whose countries do not add up to their 'Total' column
-- (the 'Unclassified' row is exempt: in the 2025 edition it differs by up to 5 in 2016, 2017, 2019 and 2020); a
-- table title that does not name its indicator or, for a year sheet, its year. A layout change that moves a header,
-- a label or a column fails here instead of loading wrong cells.
with c as (
    select * from {{ ref('epo_statistics_counts') }}
),

editions as (
    select edition from c group by edition
),

selection as (
    select t.table_code
    from (
        values
        ('applications_by_country'), ('grants_by_country'), ('applications_by_field'), ('grants_by_field'),
        ('applications_by_field_country'), ('grants_by_field_country'), ('poland_regions')
    ) as t (table_code)
),

missing_tables as (
    select
        e.edition,
        s.table_code,
        null as period_year,
        'table missing' as problem
    from editions as e
    cross join selection as s
    where not exists (select 1 from c where c.edition = e.edition and c.table_code = s.table_code)
),

spans as (
    select
        edition,
        table_code,
        unnest(generate_series(min(period_year), max(period_year))) as period_year
    from c
    group by edition, table_code
),

totals as (
    select
        edition,
        table_code,
        indicator,
        period_year,
        coalesce(field_name, '_T') as field_key,
        max(value) filter (where area_label = 'Total') as total,
        sum(value) filter (where area_label <> 'Total') as parts,
        count(*) filter (where area_label = 'Total' and value is not null) as total_rows
    from c
    group by all
),

no_total as (
    select
        s.edition,
        s.table_code,
        s.period_year,
        'no Total' as problem
    from spans as s
    where
        s.table_code not like '%_by_field'
        and not exists (
            select 1 from totals as t
            where
                t.edition = s.edition and t.table_code = s.table_code and t.period_year = s.period_year
                and t.total_rows > 0
        )
),

sums as (
    select
        edition,
        table_code,
        period_year,
        'parts ' || cast(parts as varchar) || ' <> Total ' || cast(total as varchar) || ' (' || field_key || ')'
            as problem
    from totals
    where
        (table_code like '%_by_country' or table_code = 'poland_regions'
            or (table_code like '%_field_country' and field_key <> 'Unclassified'))
        and parts is distinct from total
),

fields as (
    select
        s.edition,
        s.table_code,
        s.period_year,
        count(distinct c.field_name) filter (where c.field_name <> 'Unclassified' and c.value is not null)
            as named_fields,
        sum(c.value) filter (where c.table_code like '%_by_field') as field_sum
    from spans as s
    left join c on c.edition = s.edition and c.table_code = s.table_code and c.period_year = s.period_year
    where s.table_code like '%_by_field%'
    group by all
),

few_fields as (
    select
        edition,
        table_code,
        period_year,
        'named fields ' || cast(named_fields as varchar) as problem
    from fields
    where named_fields <> 35
),

field_vs_total as (
    select
        f.edition,
        f.table_code,
        f.period_year,
        'fields ' || cast(f.field_sum as varchar) || ' <> country table Total ' || cast(t.total as varchar) as problem
    from fields as f
    left join totals as t
        on
            f.edition = t.edition and t.table_code = replace(f.table_code, '_by_field', '_by_country')
            and f.period_year = t.period_year and t.field_key = '_T'
    where f.table_code like '%_by_field' and f.field_sum is distinct from t.total
),

titles as (
    select
        edition,
        table_code,
        period_year,
        'title ' || table_title as problem
    from c
    where
        not coalesce(
            case
                when indicator = 'grants' then regexp_matches(table_title, 'granted', 'i')
                else regexp_matches(table_title, 'patent applications', 'i')
            end
            and (table_code not like '%_field_country' or contains(table_title, cast(period_year as varchar))),
            false
        )
    group by all
)

select * from missing_tables
union all
select * from no_total
union all
select * from sums
union all
select * from few_fields
union all
select * from field_vs_total
union all
select * from titles
