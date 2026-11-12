{{ config(alias='build_knf_banking', tags=['gold_block']) }}
{# Gold build stage: KNF monthly banking-sector data (register PLG-017, Silver knf_banking_series), the smallest
   selection of the register: Poland, current workbook only (January 2018 on; the archive to December 2017 is
   another accounting basis and stays in Silver). Breakdown bank_group: sector (the banking sector, total),
   commercial, branches (of credit institutions), cooperative. Values, unit and scale as the workbook stores them:
   zloty for amounts ("Dane w zł"), a fraction for the capital ratio ('%'-formatted, 0.1886 = 18.86 %).
   Blocks (one frequency per measure key):
   balance_sheet     total_assets            "Aktywa, razem" (Bilans), month ends (M)
   income_statement  net_profit_ytd          "Zysk (strata) roku bieżącego" (RZiS), cumulative from January (M)
   loans             nonfin_non_impaired     non-financial sector, portfolio B gross carrying amount without
                                             impairment, stages 1 and 2 (Należności_1, "SEKTOR NIEFINANSOWY") (M)
                     nonfin_impaired_stage3  the same with impairment, stage 3 (M)
   capital_adequacy  total_capital_ratio     "Łączny współczynnik kapitałowy" (Adekwatność), quarter ends (Q) #}
with selection as (
    select * from (
        values
        ('Bilans', '', 'Aktywa, razem', 'balance_sheet', 'total_assets', 'Total assets', 'M'),
        (
            'RZiS', '', 'Zysk (strata) roku bieżącego', 'income_statement', 'net_profit_ytd',
            'Net profit (loss) of the current year, cumulative from January', 'M'
        ),
        (
            'Należności_1', 'SEKTOR NIEFINANSOWY', 'Należności bez utraty wartości, w tym:', 'loans',
            'nonfin_non_impaired', 'Loans and receivables, non-financial sector, without impairment (stages 1 and 2)',
            'M'
        ),
        (
            'Należności_1', 'SEKTOR NIEFINANSOWY', 'Należności ze stwierdzoną utratą wartości - Faza 3', 'loans',
            'nonfin_impaired_stage3', 'Loans and receivables, non-financial sector, impaired (stage 3)', 'M'
        ),
        (
            'Adekwatność', '', 'Łączny współczynnik kapitałowy', 'capital_adequacy', 'total_capital_ratio',
            'Total capital ratio', 'Q'
        )
    ) as t (sheet_name, section_label, item_label, block_id, code, name, frequency)
),

blocks as (
    select * from (
        values
        ('balance_sheet', 'KNF banking sector: balance sheet'),
        ('income_statement', 'KNF banking sector: income statement'),
        ('loans', 'KNF banking sector: loans to the non-financial sector'),
        ('capital_adequacy', 'KNF banking sector: capital adequacy')
    ) as t (block_id, block_name)
),

geo as (
    select
        source_code,
        geography_key
    from {{ ref('poc_map_geography') }}
    where source_system = 'knf_banking'
),

picked as (
    -- item_occurrence 1: the label once per section and bank-group block; Silver's unique test guards the key
    select
        x.block_id,
        x.code,
        x.name,
        s.frequency,
        s.bank_group,
        s.bank_group_code,
        s.unit_code as silver_unit,
        s.period_end,
        s.period_start,
        s.value,
        s.source_version,
        s.raw_sha256
    from {{ ref('knf_banking_series') }} as s
    inner join selection as x
        on
            s.sheet_name = x.sheet_name
            and coalesce(s.section_label, '') = x.section_label
            and s.item_label = x.item_label
            and s.frequency = x.frequency
    where
        s.entity = 'current'
        and s.item_occurrence = 1
        and s.bank_group_code in ('sector', 'commercial', 'branches', 'cooperative')
        and s.value is not null
)

select
    'knf_banking' as source_system,
    p.block_id,
    b.block_name,
    'knf_banking:' || p.block_id || ':' || case when p.silver_unit = 'ratio' then 'RATIO' else 'PLN' end || ':' || p.code
        as measure_key,
    p.name as measure_name,
    p.code as measure_codes,
    case when p.silver_unit = 'ratio' then 'RATIO' else 'PLN' end as unit_code,
    case when p.silver_unit = 'ratio' then 'ratio (fraction; printed as %)' else 'zł' end as unit_name,
    'PL' as geography_source_code,
    coalesce(g.geography_key, '_X') as geography_key,
    p.frequency,
    p.period_start as date_key,
    case
        when p.frequency = 'Q' then year(p.period_end) || '-Q' || quarter(p.period_end)
        else strftime(p.period_end, '%Y-%m')
    end as period_label,
    cast(to_json(map(['bank_group'], [p.bank_group_code])) as varchar) as breakdown_json,
    cast(to_json(map(['bank_group'], [p.bank_group])) as varchar) as breakdown_label_json,
    cast(null as varchar) as sex_code,
    p.bank_group_code = 'sector' as is_total,
    p.value,
    'ok' as observation_status_key,
    p.source_version,
    p.raw_sha256
from picked as p
inner join blocks as b on p.block_id = b.block_id
left join geo as g on g.source_code = 'PL'
