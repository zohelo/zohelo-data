{# Ministry of Finance local-government budget summaries (register PLG-010): national totals per unit type and
   quarter, year to date, from the newest copy of each summary workbook (bronze.mf_jst_budgets_cells).
   Grain: unit_type x period_date x section x item_label x item_occurrence x measure_code.
   - DW workbooks (sheet doch_wyd): item rows have a label in column B and amounts in column C (plan after changes,
     measure plan) and D (executed, measure executed), in PLN. The section is the column D header of the nearest
     "Wyszczególnienie" row above (revenue: "Dochody wykonane", expenditure: "Wydatki wykonane", financing: other).
     Rows "Planowany"/"Wykonany" (counts of units with a surplus or deficit) are not amounts and are left out.
   - ZN workbooks (sheet zob_nal): rows whose label starts with a report code (E, E1, E1.1 ... liabilities; N1 ...
     receivables) and column B (total), in PLN; item_label = the code, measure total.
   item_label is the source label with whitespace collapsed; item_occurrence numbers repeated labels in a section
   ("w tym: inwestycyjne"). Cells that are not numbers ("#") are left out. #}
with cells as (
    select *
    from {{ source('mf_jst_budgets', 'mf_jst_budgets_cells') }}
    qualify rank() over (partition by entity order by source_version desc, ingested_at desc) = 1
),

rows_wide as (
    select
        entity,
        sheet_name,
        row_number,
        any_value(source_version) as source_version,
        any_value(raw_sha256) as raw_sha256,
        max(case when column_number = 1 then value_text end) as col_a,
        max(case when column_number = 2 then value_text end) as col_b,
        max(case when column_number = 3 then value_text end) as col_c,
        max(case when column_number = 4 then value_text end) as col_d
    from cells
    where sheet_name in ('doch_wyd', 'zob_nal')
    group by entity, sheet_name, row_number
),

dw as (
    select
        *,
        trim(regexp_replace(col_b, '\s+', ' ', 'g')) as label,
        last_value(
            case
                when trim(col_b) like 'Wyszczególnienie%' and lower(col_d) like 'dochody%' then 'revenue'
                when trim(col_b) like 'Wyszczególnienie%' and lower(col_d) like 'wydatki%' then 'expenditure'
                when trim(col_b) like 'Wyszczególnienie%' then 'financing'
            end ignore nulls
        ) over (partition by entity order by row_number rows between unbounded preceding and current row) as section
    from rows_wide
    where sheet_name = 'doch_wyd'
),

dw_items as (
    select
        *,
        row_number() over (partition by entity, section, label order by row_number) as item_occurrence
    from dw
    where
        section is not null
        and label not in ('Planowany', 'Wykonany')
        and (try_cast(col_c as double) is not null or try_cast(col_d as double) is not null)
),

zn_items as (
    select
        *,
        regexp_extract(trim(col_a), '^([EN][0-9.]*)\s', 1) as code
    from rows_wide
    where
        sheet_name = 'zob_nal'
        and regexp_matches(trim(col_a), '^[EN][0-9.]*\s')
        and try_cast(col_b as double) is not null
),

amounts as (
    select
        entity,
        section,
        label as item_label,
        item_occurrence,
        'plan' as measure_code,
        try_cast(col_c as double) as value_pln,
        source_version,
        raw_sha256
    from dw_items
    union all
    select
        entity,
        section,
        label as item_label,
        item_occurrence,
        'executed' as measure_code,
        try_cast(col_d as double) as value_pln,
        source_version,
        raw_sha256
    from dw_items
    union all
    select
        entity,
        case when code like 'E%' then 'liabilities' else 'receivables' end as section,
        code as item_label,
        1 as item_occurrence,
        'total' as measure_code,
        try_cast(col_b as double) as value_pln,
        source_version,
        raw_sha256
    from zn_items
)

select
    split_part(entity, '_', 1) as unit_type,
    split_part(entity, '_', 2) as report_family,
    cast(split_part(entity, '_', 3) as integer) as report_year,
    cast(substr(split_part(entity, '_', 4), 2) as integer) as report_quarter,
    make_date(
        cast(split_part(entity, '_', 3) as integer), cast(substr(split_part(entity, '_', 4), 2) as integer) * 3 - 2, 1
    )
        as period_date,
    entity,
    section,
    item_label,
    item_occurrence,
    measure_code,
    value_pln,
    source_version,
    raw_sha256
from amounts
where value_pln is not null
