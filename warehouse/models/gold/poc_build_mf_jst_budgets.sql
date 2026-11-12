{{ config(alias='build_mf_jst_budgets', tags=['gold_block']) }}
{# Gold build stage: Ministry of Finance local-government budget summaries (register PLG-010) as quarterly
   observations, year to date (cumulative from January; the measure names say so). Block 'jst_budget': executed
   revenue, executed expenditure, the budget result (revenue minus expenditure) and liabilities (debt, code E) of
   Poland's local-government units, in PLN; the unit type is the breakdown (jst = all units, the total). One
   frequency (Q) per measure_key. "WYDATKI OGÓŁEM UE" (EU-funded expenditure) is another item, not matched. #}
with picked as (
    select
        *,
        case
            when
                section = 'revenue' and upper(item_label) = 'DOCHODY OGÓŁEM' and item_occurrence = 1
                and measure_code = 'executed' then 'revenue_executed'
            when
                section = 'expenditure'
                and regexp_replace(upper(item_label), '\s*Z TEGO:$', '') = 'WYDATKI OGÓŁEM'
                and item_occurrence = 1
                and measure_code = 'executed' then 'expenditure_executed'
            when
                section = 'expenditure' and upper(item_label) = 'WYNIK' and item_occurrence = 1
                and measure_code = 'executed' then 'result_executed'
            when section = 'liabilities' and item_label = 'E' and measure_code = 'total' then 'liabilities_total'
        end as measure_code_gold
    from {{ ref('mf_jst_budget_summary') }}
)

select
    'mf_jst_budgets' as source_system,
    'jst_budget' as block_id,
    'MF local-government budget summaries (year to date)' as block_name,
    'mf_jst_budgets:jst_budget:pln:' || measure_code_gold as measure_key,
    case measure_code_gold
        when 'revenue_executed' then 'Local-government revenue, executed, year to date (PLN)'
        when 'expenditure_executed' then 'Local-government expenditure, executed, year to date (PLN)'
        when 'result_executed' then 'Local-government budget result (revenue minus expenditure), year to date (PLN)'
        when 'liabilities_total' then 'Local-government liabilities (debt instruments), end of quarter (PLN)'
    end as measure_name,
    measure_code_gold as measure_codes,
    'pln' as unit_code,
    'PLN' as unit_name,
    cast(null as varchar) as geography_source_code,
    'PL' as geography_key,
    'Q' as frequency,
    period_date as date_key,
    report_year || '-Q' || report_quarter as period_label,
    cast(to_json(map(['unit_type'], [unit_type])) as varchar) as breakdown_json,
    cast(to_json(map(['unit_type'], [unit_type])) as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    unit_type = 'jst' as is_total,
    value_pln as value,
    'ok' as observation_status_key,
    source_version,
    raw_sha256
from picked
where measure_code_gold is not null
